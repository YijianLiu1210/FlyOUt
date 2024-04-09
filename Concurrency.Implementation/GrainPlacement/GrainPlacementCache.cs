using Concurrency.Interface.GrainPlacement;
using System;
using System.Collections.Generic;
using System.Threading;
using StackExchange.Redis;
using Utilities;
using Orleans;
using System.Linq;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GrainPlacementCache : IGrainPlacementCache
    {
        bool hierarchicalCoord;

        string globalSiloAddress;
        SemaphoreSlim globalSiloLock;

        SemaphoreSlim grainInfoLock;
        Dictionary<Guid, string> grainIDToSilo;
        Dictionary<Guid, Guid> grainIDToMW;

        SemaphoreSlim coordInfoLock;
        Dictionary<string, List<Guid>> coordsPerSilo;

        SemaphoreSlim pmInfoLock;
        List<Guid> allPMs;                             // all GrainPlacementManager in the whole system
        Dictionary<string, List<Guid>> pmPerSilo;

        SemaphoreSlim mwInfoLock;
        Dictionary<string, List<Guid>> mwPerSilo;

        bool initializationDone;
        readonly IDatabase siloInfo_db;
        readonly IDatabase grainPlacement_db;
        readonly IServer redisServer;

        readonly Random rndForCoord = new Random();
        readonly Random rndForPM = new Random();
        readonly Random rndForMW = new Random();

        public GrainPlacementCache(IConnectionMultiplexer redis, IServer redisServer)
        {
            globalSiloAddress = "";
            globalSiloLock = new SemaphoreSlim(1);
            grainInfoLock = new SemaphoreSlim(1);
            coordInfoLock = new SemaphoreSlim(1);
            pmInfoLock = new SemaphoreSlim(1);
            mwInfoLock = new SemaphoreSlim(1);
            grainIDToSilo = new Dictionary<Guid, string>();
            grainIDToMW = new Dictionary<Guid, Guid>();
            coordsPerSilo = new Dictionary<string, List<Guid>>();
            allPMs = new List<Guid>();
            mwPerSilo = new Dictionary<string, List<Guid>>();
            pmPerSilo = new Dictionary<string, List<Guid>>();
            siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);
            this.redisServer = redisServer;
            initializationDone = false;
        }

        public void SetHierarchicalCoord(bool hierarchicalCoord)
        {
            this.hierarchicalCoord = hierarchicalCoord;
            Console.WriteLine($"GrainPlacementCache: set hierarchicalCoord = {hierarchicalCoord}");
        } 
        public bool GetHierarchicalCoord() => hierarchicalCoord;

        public void PrepareCache(bool isGrainMigrationExp, string siloAddress)
        {
            GetGlobalSiloAddress();

            Console.WriteLine($"Write all grain placement info into cache, grainIDToSilo already contains {grainIDToSilo.Count} entries. ");

            if (Constants.benchmark == BenchmarkType.SMALLBANK)
            {
                var registeredSilo = Helper.GetLocalSiloList(siloInfo_db);
                grainInfoLock.Wait();
                for (int i = 0; i < (isGrainMigrationExp ? registeredSilo.Count / 2 : registeredSilo.Count); i++)
                {
                    var grains = Helper.GetGrainsOfSilo(i);
                    foreach (var id in grains) grainIDToSilo[id] = registeredSilo[i];
                }
                grainInfoLock.Release();
            }
            else if (Constants.benchmark == BenchmarkType.TPCC)
            {
                var keys = redisServer.Keys(Constants.Redis_GrainPlacementMap, Constants.GrainIDPrefix + "*");
                Console.WriteLine($"find {keys.Count()} grain IDs");
                foreach (var key in keys)
                {
                    var grainID = Guid.Parse(key.ToString().Split("+")[1]);

                    var silo = grainPlacement_db.HashGet(key, "SiloAddress").ToString();
                    if (string.IsNullOrEmpty(silo)) throw new SnapperStorageException($"SiloAddress info of grain {grainID} is not in Redis {Constants.Redis_GrainPlacementMap}, key = {key}");
                    grainIDToSilo[grainID] = silo;
                }
            }

            initializationDone = true;
            Console.WriteLine($"Get info of {grainIDToSilo.Count} grains");
        }

        List<Guid> LoadListInfo(string listName)
        {
            var list = new List<Guid>();

            var index = 0;
            var guid = siloInfo_db.ListGetByIndex(listName, index);
            while (guid != RedisValue.Null)
            {
                list.Add(Guid.Parse(guid.ToString()));
                index++;
                guid = siloInfo_db.ListGetByIndex(listName, index);
            }

            if (list.Count == 0) throw new Exception($"Read {listName} from Redis, list is empty");
            return list;
        }

        public Dictionary<string, Guid> GetOneRandomLocalCoordPerSilo()
        {
            // get the list of all registered local silo
            var index = 0;
            var localCoordPerSilo = new Dictionary<string, Guid>();
            var siloAddress = siloInfo_db.ListGetByIndex("LocalSiloList", index);
            while (siloAddress != RedisValue.Null)
            {
                localCoordPerSilo.Add(siloAddress, GetOneRandomCoordInSilo(siloAddress));
                index++;
                siloAddress = siloInfo_db.ListGetByIndex("LocalSiloList", index);
            }
            return localCoordPerSilo;
        }

        public List<Guid> GetAllPM()
        {
            if (allPMs.Count == 0)
            {
                pmInfoLock.Wait();
                if (allPMs.Count == 0) allPMs = LoadListInfo("PlacementManagerList");
                pmInfoLock.Release();
            }
            return allPMs;
        }

        public string GetGlobalSiloAddress()
        {
            if (globalSiloAddress == "")
            {
                globalSiloLock.Wait();
                if (globalSiloAddress == "") 
                    globalSiloAddress = siloInfo_db.StringGet(Constants.GlobalSilo);
                globalSiloLock.Release();
            } 
            return globalSiloAddress;
        }

        public void UpdateUserGrainInfo(Guid grainID, string siloAddress)
        {
            grainInfoLock.Wait();
            grainIDToSilo[grainID] = siloAddress;
            if (grainIDToMW.ContainsKey(grainID)) grainIDToMW.Remove(grainID);
            grainInfoLock.Release();
        }

        public void AddUserGrain(Guid grainID, string silo)
        {
            grainInfoLock.Wait();
            grainIDToSilo[grainID] = silo;
            grainInfoLock.Release();
        }

        public Guid GetMigrationWorker(Guid grainID)
        {
            if (grainIDToMW.ContainsKey(grainID) == false)
            {
                grainInfoLock.Wait();
                if (grainIDToMW.ContainsKey(grainID) == false)  // check again after get the lock
                {
                    var str = grainPlacement_db.HashGet(Constants.GrainIDPrefix + grainID.ToString(), "MigrationWorker").ToString();
                    if (string.IsNullOrEmpty(str)) throw new SnapperStorageException($"MigrationWorker info of grain {grainID} is not in Redis {Constants.Redis_GrainPlacementMap}");
                    grainIDToMW[grainID] = Guid.Parse(str);
                }
                grainInfoLock.Release();
            }
            return grainIDToMW[grainID];
        }

        public string GetSilo(Guid grainID)
        {
            if (grainIDToSilo.ContainsKey(grainID) == false)
            {
                grainInfoLock.Wait();
                if (grainIDToSilo.ContainsKey(grainID) == false)  // check again after get the lock
                {
                    if (initializationDone) Console.WriteLine($"GetSilo: read from redis");
                    var silo = grainPlacement_db.HashGet(Constants.GrainIDPrefix + grainID.ToString(), "SiloAddress").ToString();
                    if (string.IsNullOrEmpty(silo)) throw new SnapperStorageException($"SiloAddress info of grain {grainID} is not in Redis {Constants.Redis_GrainPlacementMap}");
                    grainIDToSilo[grainID] = silo;
                }
                grainInfoLock.Release();
            }
            
            return grainIDToSilo[grainID];
        }

        public Guid GetCoordNeighborID(string siloAddress, Guid coordID)
        {
            if (coordsPerSilo.ContainsKey(siloAddress) == false)
            {
                coordInfoLock.Wait();
                if (coordsPerSilo.ContainsKey(siloAddress) == false)
                    coordsPerSilo[siloAddress] = LoadListInfo(Constants.CoordInfoPrefix + siloAddress);
                coordInfoLock.Release();
            }
              
            var coordList = coordsPerSilo[siloAddress];
            if (coordList.Contains(coordID) == false) throw new Exception($"GetCoordNeighborID: coordID {coordID} is not in coordList");
            var index = 0;
            while (coordList[index] != coordID) index++;
            if (index == coordList.Count - 1) return coordList[0];
            else return coordList[index + 1];
        }

        public List<Guid> GetAllCoordsInSilo(string siloAddress)
        {
            if (coordsPerSilo.ContainsKey(siloAddress) == false) return new List<Guid>();
            else return coordsPerSilo[siloAddress];
        }

        public Guid GetOneRandomCoordInSilo(string siloAddress)
        {
            if (hierarchicalCoord == false) siloAddress = GetGlobalSiloAddress();

            if (coordsPerSilo.ContainsKey(siloAddress) == false)
            {
                coordInfoLock.Wait();
                if (coordsPerSilo.ContainsKey(siloAddress) == false)
                {
                    if (initializationDone) Console.WriteLine($"GetOneRandomCoordInSilo: read from redis");
                    coordsPerSilo[siloAddress] = LoadListInfo(Constants.CoordInfoPrefix + siloAddress);
                }    
                coordInfoLock.Release();
            }
                
            var coordList = coordsPerSilo[siloAddress];
            return coordList[rndForCoord.Next(0, coordList.Count)];
        }

        public Guid GetOneCoordInSiloByID(int numLocalSilo, string siloAddress, Guid placementManagerID)
        {
            if (coordsPerSilo.ContainsKey(siloAddress) == false)
            {
                coordInfoLock.Wait();
                if (coordsPerSilo.ContainsKey(siloAddress) == false)
                {
                    if (initializationDone) Console.WriteLine($"GetOneCoordInSiloByIndex: read from redis");
                    coordsPerSilo[siloAddress] = LoadListInfo(Constants.CoordInfoPrefix + siloAddress);
                }
                coordInfoLock.Release();
            }

            var coordList = coordsPerSilo[siloAddress];
            var pmID = Helper.ConvertGuidToInt(placementManagerID);
            var siloID = pmID / Constants.numGrainPlacementManagerPerSilo;   // get the siloID
            
            // this can only work when numGC = numLocalsilo
            if (Constants.numGlobalCoordPerLocalSilo != 1) throw new Exception($"GetOneCoordInSiloByID: numGlobalCoordPerLocalSilo must be 1");
            var coordID = Helper.ConvertIntToGuid((siloID % 2) * coordList.Count / 2 + siloID / 2);
            if (coordList.Contains(coordID) == false) throw new Exception($"GetOneCoordInSiloByID: coordID {coordID} is not in coordList");
            return coordID;
        }

        public List<Guid> GetAllPMInSilo(string siloAddress)
        {
            if (pmPerSilo.ContainsKey(siloAddress) == false)
            {
                pmInfoLock.Wait();
                if (pmPerSilo.ContainsKey(siloAddress) == false) pmPerSilo[siloAddress] = LoadListInfo("PlacementManagerList-" + siloAddress);
                pmInfoLock.Release();
            }
            return pmPerSilo[siloAddress];
        }

        public Guid GetOneRandomPMInSilo(string siloAddress)
        {
            var pmList = GetAllPMInSilo(siloAddress);
            return pmList[rndForPM.Next(0, pmList.Count)];
        }

        public Guid GetOneRandomMWInSilo(string siloAddress)
        {
            if (mwPerSilo.ContainsKey(siloAddress) == false)
            {
                mwInfoLock.Wait();
                if (mwPerSilo.ContainsKey(siloAddress) == false)
                    mwPerSilo[siloAddress] = LoadListInfo("MigrationWorkerList-" + siloAddress);
                mwInfoLock.Release();
            }
                
            var mwList = mwPerSilo[siloAddress];
            return mwList[rndForMW.Next(0, mwList.Count)];
        }
    }
}