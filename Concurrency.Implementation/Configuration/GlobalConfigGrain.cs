using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.GrainPlacement;
using System;
using StackExchange.Redis;
using System.Diagnostics;
using Orleans.Concurrency;

namespace Concurrency.Implementation.Configuration
{
    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.GlobalConfig)]
    public class GlobalConfigGrain : Grain, IGlobalConfigGrain
    {
        bool configurationDone;
        List<ILocalConfigGrain> localConfigGrains;
        readonly ILoggingProtocol log;                // this logger group is only accessible within this silo host
        readonly IGrainPlacementCache grainPlacementCache;
        readonly IDatabase siloInfo_db;
        readonly IDatabase grainPlacement_db;

        int numLocalSilo;
        bool hierarchicalCoord;

        public GlobalConfigGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementCache, IConnectionMultiplexer redis)
        {
            Console.WriteLine($"GlobalConfigGrain is activated");
            configurationDone = false;
            this.log = log;
            this.grainPlacementCache = grainPlacementCache;
            siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);
        }

        public Task<int> getNumLocalSilo() => Task.FromResult(numLocalSilo);

        public async Task UpdateUserGrainInfoInCache(Guid grainID, string siloAddress)
        {
            var tasks = new List<Task>();
            foreach (var configGrain in localConfigGrains) tasks.Add(configGrain.UpdateUserGrainInfoInCache(grainID, siloAddress));
            await Task.WhenAll(tasks);
        }

        public async Task PrepareCache(bool isGrainMigrationExp)
        {
            if (hierarchicalCoord == false) return;
            var tasks = new List<Task>();
            foreach (var configGrain in localConfigGrains) tasks.Add(configGrain.PrepareCache(isGrainMigrationExp));
            await Task.WhenAll(tasks);
        }

        public async Task CheckGC()
        {
            // forward the request to all local config grains if have any
            foreach (var configGrain in localConfigGrains) await configGrain.CheckGC();

            if (hierarchicalCoord == false)
            {
                var localCoords = grainPlacementCache.GetAllCoordsInSilo(RuntimeIdentity);
                foreach (var guid in localCoords)
                {
                    var coord = GrainFactory.GetGrain<ILocalCoordGrain>(guid);
                    await coord.CheckGC();
                }
            }
            else
            {
                // checkGC for all global coordinators if have any
                var globalCoords = grainPlacementCache.GetAllCoordsInSilo(RuntimeIdentity);
                foreach (var guid in globalCoords)
                {
                    var coord = GrainFactory.GetGrain<IGlobalCoordGrain>(guid);
                    await coord.CheckGC();
                }
            }
        }

        public async Task ConfigGlobalEnv(int numLocalSilo, bool isLoggingEnabled, bool hierarchicalCoord, bool optimizeCommit, Dictionary<string, string> tpccGrainNames)
        {
            if (configurationDone)
            {
                Debug.Assert(this.numLocalSilo == numLocalSilo && this.hierarchicalCoord == hierarchicalCoord);
                await InitAllLogging(isLoggingEnabled);
                return;
            }

            // the rest only need to be set up when first time start the silo
            this.numLocalSilo = numLocalSilo;
            this.hierarchicalCoord = hierarchicalCoord;
            localConfigGrains = new List<ILocalConfigGrain>();
            for (int i = 0; i < numLocalSilo; i++)
            {
                var configGrain = GrainFactory.GetGrain<ILocalConfigGrain>(Guid.NewGuid());
                localConfigGrains.Add(configGrain);
                await configGrain.Init(hierarchicalCoord, tpccGrainNames);
            }
            if (numLocalSilo != 1) grainPlacementCache.SetHierarchicalCoord(hierarchicalCoord, tpccGrainNames);

            await InitAllLogging(isLoggingEnabled);

            if (hierarchicalCoord == false)   // all local coordinators are located in the global silo
            {
                await ConfigLocalCoordinator(Constants.numLocalCoordPerSilo * numLocalSilo, optimizeCommit);
                for (var siloID = 0; siloID < numLocalSilo; siloID++)
                {
                    await localConfigGrains[siloID].ConfigPlacementManager(numLocalSilo, siloID);
                    await localConfigGrains[siloID].ConfigMigrationWorker(siloID);
                }
            }
            else  // a group of global coordinators are located in the global silo
            {
                await ConfigGlobalCoordinator(Constants.numGlobalCoordPerLocalSilo * numLocalSilo);
                for (var siloID = 0; siloID < numLocalSilo; siloID++)
                {
                    await localConfigGrains[siloID].ConfigLocalCoordinator(siloID, optimizeCommit);
                    await localConfigGrains[siloID].ConfigPlacementManager(numLocalSilo, siloID);
                    await localConfigGrains[siloID].ConfigMigrationWorker(siloID);
                }
            }

            configurationDone = true;
        }

        async Task InitAllLogging(bool isLoggingEnabled)
        {
            // reset logging in the global silo
            await log.Init(isLoggingEnabled, true, 0, numLocalSilo, hierarchicalCoord);
            // reset logging in each local silo
            for (int siloID = 0; siloID < numLocalSilo; siloID++)
                await localConfigGrains[siloID].ConfigLogging(isLoggingEnabled, siloID, numLocalSilo);
        }

        async Task ConfigLocalCoordinator(int num, bool optimizeCommit)
        {
            var guids = new List<Guid>();
            // Generate IDs for all local coordinators and write the placement info into redis
            for (int i = 0; i < num; i++)
            {
                var guid = Helper.ConvertIntToGuid(i);
                await siloInfo_db.ListRightPushAsync(Constants.CoordInfoPrefix + RuntimeIdentity, guid.ToString());
                await grainPlacement_db.StringSetAsync(Constants.CoordIDPrefix + guid.ToString(), RuntimeIdentity);
                guids.Add(guid);
            }

            var tasks = new List<Task>();
            foreach (var guid in guids)
            {
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(guid);
                tasks.Add(coord.Init(optimizeCommit));
            }
            await Task.WhenAll(tasks);

            // Inject token to one of the local coordinators
            var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(guids[0]);
            _ = localCoord.PassToken(new LocalToken());
        }

        async Task ConfigGlobalCoordinator(int num)
        {
            var guids = new List<Guid>();
            // Generate IDs for all global coordinators and write the global info into redis
            for (int i = 0; i < num; i++)
            {
                var guid = Helper.ConvertIntToGuid(i);
                await siloInfo_db.ListRightPushAsync(Constants.CoordInfoPrefix + RuntimeIdentity, guid.ToString());
                guids.Add(guid);
            }

            var tasks = new List<Task>();
            foreach (var guid in guids)
            {
                var coord = GrainFactory.GetGrain<IGlobalCoordGrain>(guid);
                tasks.Add(coord.Init());
            }
            await Task.WhenAll(tasks);

            // Inject the token to one of the global coordinators
            var globalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(guids[0]);
            _ = globalCoord.PassToken(new BasicToken());
        }

        public async Task ConfigGlobalBatchSize(int batchSizeInMSecsBasic)
        {
            Debug.Assert(numLocalSilo != 0);
            var globalCoords = grainPlacementCache.GetAllCoordsInSilo(RuntimeIdentity);
            foreach (var guid in globalCoords)
            {
                var coord = GrainFactory.GetGrain<IGlobalCoordGrain>(guid);
                await coord.SetBatchSize(numLocalSilo, batchSizeInMSecsBasic);
            }
        }
    }
}