using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SnapperExperimentWorker;
using SmallBank.Interfaces;
using Orleans;
using Concurrency.Interface.Configuration;
using Utilities;
using System.Threading;
using StackExchange.Redis;
using System.Diagnostics;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using System.IO;
using Amazon;
using TPCC.Interfaces;
using TPCC.Grains;
using Concurrency.Interface.TransactionExecution;
using SmallBank.Grains;
using System.Linq;

namespace SnapperExperimentController
{
    public class ServerConnector
    {
        readonly int batchSize = 1000;
        readonly int numLocalSilo;
        readonly ImplementationType implementationType;
        readonly bool isLoggingEnabled;
        readonly bool hierarchicalCoord;
        readonly bool optimizeCommit;

        IClusterClient client;
        IGlobalConfigGrain globalConfigGrain;

        bool preparationDone;
        bool initializationFinish;
        bool setBatchSizeDone;
        bool cachePrepared;
        bool checkGCFinish;
        CountdownEvent threadFinishLoadingGrain;
        List<string> registeredSilo;
        readonly IServer server;
        readonly IDatabase grainPlacement_db;
        readonly IDatabase siloInfo_db;

        readonly bool eventual;
        Dictionary<string, Dictionary<string, HashSet<int>>> grainsPerSilo;    // silo name, grain name, grian ID

        // for SmallBank only
        readonly string grainName;

        // for TPCC only
        readonly string itemGrainName;
        readonly string warehouseGrainName;
        readonly string districtGrainName;
        readonly string customerGrainName;
        readonly string stockGrainName;
        readonly string orderGrainName;
        readonly Dictionary<string, string> tpccGrainNames;

        public ServerConnector(int numLocalSilo, ImplementationType implementationType, bool isLoggingEnabled, string redis_ConnectionString,
            bool hierarchicalCoord, bool optimizeCommit)
        {
            this.numLocalSilo = numLocalSilo;
            this.implementationType = implementationType;
            this.isLoggingEnabled = isLoggingEnabled;
            this.hierarchicalCoord = hierarchicalCoord;
            this.optimizeCommit = optimizeCommit;
            var redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { redis_ConnectionString }, AllowAdmin = true });
            server = redis.GetServer(redis_ConnectionString);
            siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);

            // ==================================================================================================================
            eventual = implementationType == ImplementationType.NONTXN;
            grainsPerSilo = new Dictionary<string, Dictionary<string, HashSet<int>>>();
            if (Constants.benchmark == BenchmarkType.TPCC)
            {
                itemGrainName = eventual ? typeof(EventualItemGrain).FullName : typeof(ItemGrain).FullName;
                warehouseGrainName = eventual ? typeof(EventualWarehouseGrain).FullName : typeof(WarehouseGrain).FullName;
                districtGrainName = eventual ? typeof(EventualDistrictGrain).FullName : typeof(DistrictGrain).FullName;
                customerGrainName = eventual ? typeof(EventualCustomerGrain).FullName : typeof(CustomerGrain).FullName;
                stockGrainName = eventual ? typeof(EventualStockGrain).FullName : typeof(StockGrain).FullName;
                orderGrainName = eventual ? typeof(EventualOrderGrain).FullName : typeof(OrderGrain).FullName;

                tpccGrainNames = new Dictionary<string, string>
                {
                    { "itemGrainName", itemGrainName },
                    { "warehouseGrainName", warehouseGrainName },
                    { "districtGrainName", districtGrainName },
                    { "customerGrainName", customerGrainName },
                    { "stockGrainName", stockGrainName },
                    { "orderGrainName", orderGrainName }
                };
            }
            else if (Constants.benchmark == BenchmarkType.SMALLBANK)
            {
                switch (implementationType)
                {
                    case ImplementationType.SNAPPER:
                        grainName = typeof(SnapperTransactionalAccountGrain).FullName;
                        break;
                    case ImplementationType.NONTXN:
                        grainName = typeof(NonTransactionalAccountGrain).FullName;
                        break;
                    case ImplementationType.ORLEANSTXN:
                        grainName = typeof(OrleansTransactionalAccountGrain).FullName;
                        break;
                }
            }
            // ==================================================================================================================

            preparationDone = false;
            PrepareEnv();
            while (preparationDone == false) Thread.Sleep(100);
        }

        async void PrepareEnv()
        {
            // flush all data stored in Redis cluster
            server.FlushAllDatabases();
            Console.WriteLine($"The data in Redis cluster is all deleted");

            // delete the existing silo membership table in DynamoDB
            string AccessKey, SecretKey;
            using (var file = new StreamReader(Constants.credentialFile))
            {
                var ServiceRegion = file.ReadLine();
                Debug.Assert(ServiceRegion == "eu-north-1");
                AccessKey = file.ReadLine();
                SecretKey = file.ReadLine();
            }
            var dynamoDBClient = new AmazonDynamoDBClient(AccessKey, SecretKey, RegionEndpoint.EUNorth1);

            try
            {
                await dynamoDBClient.DeleteTableAsync(Constants.SiloMembershipTable);
                // wait until the table has completely deleted
                var exception = false;
                while (exception == false)
                {
                    try
                    {
                        await dynamoDBClient.DescribeTableAsync(Constants.SiloMembershipTable);
                    }
                    catch (ResourceNotFoundException)
                    {
                        exception = true;
                    }
                }
                Console.WriteLine($"The {Constants.SiloMembershipTable} is deleted");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine($"The {Constants.SiloMembershipTable} does not exist, no need to delete");
            }

            preparationDone = true;
        }

        public void InitiateClientAndServer()
        {
            initializationFinish = false;
            InitiateClientAndServerAsync();
            while (!initializationFinish) Thread.Sleep(100);
        }

        public void LoadGrains(bool isGrainMigrationExp)
        {
            if (Constants.benchmark == BenchmarkType.SMALLBANK) LoadSmallBankGrains(isGrainMigrationExp);
            else if (Constants.benchmark == BenchmarkType.TPCC) LoadTPCCGrains();
        }

        public void SetGlobalBatchSize(int batchSizeInMSecsBasic)
        {
            setBatchSizeDone = false;
            SetGlobalBatchSizeAsync(batchSizeInMSecsBasic);
            while (!setBatchSizeDone) Thread.Sleep(100);
        }

        async void InitiateClientAndServerAsync()
        {
            // init silo servers
            var manager = new OrleansClientManager();
            client = await manager.StartOrleansClient();

            if (implementationType == ImplementationType.SNAPPER)
            {
                var start = DateTime.Now;
                globalConfigGrain = client.GetGrain<IGlobalConfigGrain>("GlobalConfigGrain");
                await globalConfigGrain.ConfigGlobalEnv(numLocalSilo, isLoggingEnabled, hierarchicalCoord, optimizeCommit, tpccGrainNames);
                Console.WriteLine($"Spawned the global configuration grain, it takes {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");
            }
            initializationFinish = true;
        }

        async void SetGlobalBatchSizeAsync(int batchSizeInMSecsBasic)
        {
            if (implementationType == ImplementationType.SNAPPER && hierarchicalCoord == true)
            {
                globalConfigGrain = client.GetGrain<IGlobalConfigGrain>("GlobalConfigGrain");
                await globalConfigGrain.ConfigGlobalBatchSize(batchSizeInMSecsBasic);
                Console.WriteLine($"Set global batch size as {batchSizeInMSecsBasic}ms.");
            }
            setBatchSizeDone = true;
        }

        public void PrepareCache(bool isGrainMigrationExp)
        {
            cachePrepared = false;
            PrepareCacheAsync(isGrainMigrationExp);
            while (!cachePrepared) Thread.Sleep(100);
        }

        async void PrepareCacheAsync(bool isGrainMigrationExp)
        {
            Console.WriteLine("Start prepare cache");
            if (implementationType == ImplementationType.SNAPPER)
            {
                var start = DateTime.Now;
                await globalConfigGrain.PrepareCache(isGrainMigrationExp);
                Console.WriteLine($"GrainPlacementCache is prepared for all local silos, it takes {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");
            }
            cachePrepared = true;
        }

        public void CheckGC()
        {
            if (implementationType != ImplementationType.SNAPPER) return;

            checkGCFinish = false;
            CheckGCAsync();
            while (!checkGCFinish) Thread.Sleep(100);
        }

        async void CheckGCAsync()
        {
            // check all global & local coordinators
            var tasks = new List<Task>();
            await globalConfigGrain.CheckGC();

            // check all transactional grains
            foreach (var item in grainsPerSilo)
            {
                var siloName = item.Key;
                foreach (var iitem in item.Value)
                {
                    var grainName = iitem.Key;
                    foreach (var id in iitem.Value)
                    {
                        var grain = client.GetGrain<ITransactionExecutionGrain>(Helper.ConvertIntToGuid(id), grainName);
                        tasks.Add(grain.CheckGC());

                        if (tasks.Count == batchSize)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
            }
            await Task.WhenAll(tasks);
            checkGCFinish = true;
        }

        void LoadSmallBankGrains(bool isGrainMigrationExp)
        {
            registeredSilo = Helper.GetLocalSiloList(siloInfo_db);
            Debug.Assert(registeredSilo.Count == numLocalSilo);
            var numSiloWithGrains = isGrainMigrationExp ? numLocalSilo / 2 : numLocalSilo;

            // calculate IDs of all grains in all local silos
            for (var siloID = 0; siloID < numSiloWithGrains; siloID++)
            {
                var silo = registeredSilo[siloID];
                var accountGrainsInThisSilo = Helper.GetGrainsOfSilo(siloID);
                grainsPerSilo.Add(silo, new Dictionary<string, HashSet<int>> { { grainName, accountGrainsInThisSilo.Select(guid => Helper.ConvertGuidToInt(guid)).ToHashSet() } });
            }

            // spawn multiple threads to load grains
            threadFinishLoadingGrain = new CountdownEvent(numSiloWithGrains);
            for (int i = 0; i < numSiloWithGrains; i++)
            {
                var thread = new Thread(ThreadWorkAsync);
                thread.Start(i);
            }
            threadFinishLoadingGrain.Wait();

            var numGrain = Constants.numGrainPerLocalSilo * numSiloWithGrains;
            Console.WriteLine($"Finish loading SmallBank grains, numGrains = {numGrain}");
        }

        async void ThreadWorkAsync(object obj)
        {
            var threadID = (int)obj;
            Debug.Assert(threadID < numLocalSilo);
            var silo = registeredSilo[threadID];
            var accountGrainsInThisSilo = grainsPerSilo[silo][grainName].Select(id => Helper.ConvertIntToGuid(id));

            // write the initial grain placement info to redis
            var start = DateTime.Now;
            var tasks = new List<Task>();
            foreach (var id in accountGrainsInThisSilo)
            {
                tasks.Add(grainPlacement_db.HashSetAsync(Constants.GrainIDPrefix + id.ToString(), "SiloAddress", silo));
                if (tasks.Count == batchSize)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            await Task.WhenAll(tasks);
            Console.WriteLine($"Thread {threadID} writes all grain placement info to Redis, it tasks {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");

            // activate all grains (so underMigration is set false)
            tasks.Clear();
            Console.WriteLine("activate all grains...");
            if (implementationType == ImplementationType.SNAPPER)
            {
                start = DateTime.Now;
                tasks = new List<Task>();
                foreach (var id in accountGrainsInThisSilo)
                {
                    var grain = client.GetGrain<ISnapperTransactionalAccountGrain>(id);
                    tasks.Add(grain.ActivateGrain());

                    if (tasks.Count == batchSize)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
                await Task.WhenAll(tasks);
                Console.WriteLine($"Thread {threadID} activate all grains, it tasks {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");
            }

            // initialize transactional state for all grains
            tasks.Clear();
            Console.WriteLine("initialize transactional state for all grains...");
            start = DateTime.Now;
            foreach (var id in accountGrainsInThisSilo)
            {
                switch (implementationType)
                {
                    case ImplementationType.NONTXN:
                        var etxnGrain = client.GetGrain<INonTransactionalAccountGrain>(id);
                        tasks.Add(etxnGrain.StartTransaction("Init", id));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var orltxnGrain = client.GetGrain<IOrleansTransactionalAccountGrain>(id);
                        tasks.Add(orltxnGrain.StartTransaction("Init", id));
                        break;
                    case ImplementationType.SNAPPER:
                        var sntxnGrain = client.GetGrain<ISnapperTransactionalAccountGrain>(id);
                        tasks.Add(sntxnGrain.StartTransaction("Init", id));
                        break;
                    default:
                        throw new Exception("Unknown grain implementation type");
                }

                if (tasks.Count == batchSize)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            await Task.WhenAll(tasks);
            Console.WriteLine($"Thread {threadID} finish initializing grains, it takes {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s.");

            threadFinishLoadingGrain.Signal();
        }

        void LoadTPCCGrains()
        {
            var start = DateTime.Now;
            Console.WriteLine($"Load TPCC grains...");
            registeredSilo = Helper.GetLocalSiloList(siloInfo_db);
            Debug.Assert(registeredSilo.Count == numLocalSilo);

            grainsPerSilo = TPCCManager.CalculateGrainPlacement(numLocalSilo, registeredSilo, tpccGrainNames);
            Console.WriteLine($"Finish calculating alkl grain placement info, it takes {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");
            
            /*
            for (var siloID = 0; siloID < numLocalSilo; siloID++)
            {
                start = DateTime.Now;
                var silo = registeredSilo[siloID];

                // write the initial grain placement info to redis
                
                var grainIDsInSilo = new List<Guid>();
                foreach (var item in grainsPerSilo[silo]) grainIDsInSilo.AddRange(item.Value.Select(x => Helper.ConvertIntToGuid(x)));

                var data = MessagePackSerializer.Serialize(grainIDsInSilo);
                siloInfo_db.HashSet(Constants.GeneralInfoPrefix + silo, "grainsInSilo", data);
                
                foreach (var item in grainsPerSilo[silo])
                    foreach (var id in item.Value)
                        grainPlacement_db.HashSet(Constants.GrainIDPrefix + Helper.ConvertIntToGuid(id).ToString(), "SiloAddress", silo);

                Console.WriteLine($"Finish writing grains info for silo {silo} to Redis, it takes {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");
            }
            */

            // spawn multiple threads to load grains
            var numSiloWithGrains = numLocalSilo;
            threadFinishLoadingGrain = new CountdownEvent(numSiloWithGrains);
            for (int i = 0; i < numSiloWithGrains; i++)
            {
                var thread = new Thread(ThreadWorkForTPCCAsync);
                thread.Start(i);
            }
            threadFinishLoadingGrain.Wait();

            var numGrain = 0;
            foreach (var item in grainsPerSilo) foreach (var iitem in item.Value) numGrain += iitem.Value.Count;
            Console.WriteLine($"Finish loading TPCC grains, numGrains = {numGrain}");
        }

        async void ThreadWorkForTPCCAsync(object obj)
        {
            var siloID = (int)obj;
            Debug.Assert(siloID < numLocalSilo);
            var silo = registeredSilo[siloID];
            var grainsInThisSilo = grainsPerSilo[silo];
            var minWarehouseID = siloID * Constants.NUM_W_PER_SILO;

            // STEP 1: activate all grains (so underMigration is set false)
            var start = DateTime.Now;
            var tasks = new List<Task>();
            if (implementationType == ImplementationType.SNAPPER)
            {
                tasks.Clear();
                start = DateTime.Now;
                Console.WriteLine("activate all grains...");
                foreach (var item in grainsInThisSilo)
                {
                    foreach (var id in item.Value)
                    {
                        var grain = client.GetGrain<ITransactionExecutionGrain>(Helper.ConvertIntToGuid(id), item.Key);
                        tasks.Add(grain.ActivateGrain());

                        if (tasks.Count == batchSize)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
                await Task.WhenAll(tasks);
                Console.WriteLine($"Thread {siloID} activate all grains, it tasks {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s");
            }

            // STEP 2: load ItemGrains
            for (var W_ID = minWarehouseID; W_ID < minWarehouseID + Constants.NUM_W_PER_SILO; W_ID++)
            {
                var itemGrainID = TPCCManager.GetItemGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<IEventualItemGrain>(itemGrainID);
                    await grain.StartTransaction("Init", null);
                }
                else
                {
                    var guid = Helper.ConvertIntToGuid(itemGrainID);
                    var grain = client.GetGrain<IItemGrain>(guid);
                    //await grain.StartTransaction("Init", null, new List<GrainID> { new GrainID(guid, itemGrainName) });
                    await grain.StartTransaction("Init", null);
                }
            }
            Console.WriteLine($"Finish loading {grainsInThisSilo[itemGrainName].Count} ItemGrain. ");

            // STEP 3: load WarehouseGrain
            for (var W_ID = minWarehouseID; W_ID < minWarehouseID + Constants.NUM_W_PER_SILO; W_ID++)
            {
                var warehouseGrainID = TPCCManager.GetWarehouseGrain(W_ID);
                Debug.Assert(grainsInThisSilo[warehouseGrainName].Contains(warehouseGrainID));

                if (eventual)
                {
                    var grain = client.GetGrain<IEventualWarehouseGrain>(warehouseGrainID);
                    await grain.StartTransaction("Init", W_ID);
                }
                else
                {
                    var guid = Helper.ConvertIntToGuid(warehouseGrainID);
                    var grain = client.GetGrain<IWarehouseGrain>(guid);
                    //await grain.StartTransaction("Init", W_ID, new List<GrainID> { new GrainID(guid, warehouseGrainName) });
                    await grain.StartTransaction("Init", W_ID);
                }
            }
            Console.WriteLine($"Finish loading {grainsInThisSilo[warehouseGrainName].Count} WarehouseGrain. ");

            // STEP 4: load DistrictGrain and CustomerGrain
            tasks.Clear();
            start = DateTime.Now;
            for (var W_ID = minWarehouseID; W_ID < minWarehouseID + Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    var districtGrainID = TPCCManager.GetDistrictGrain(W_ID, D_ID);
                    Debug.Assert(grainsInThisSilo[districtGrainName].Contains(districtGrainID));
                    var customerGrainID = TPCCManager.GetCustomerGrain(W_ID, D_ID);
                    Debug.Assert(grainsInThisSilo[customerGrainName].Contains(customerGrainID));

                    var input = new Tuple<int, int>(W_ID, D_ID);
                    if (eventual)
                    {
                        var districtGrain = client.GetGrain<IEventualDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("Init", input));
                        var customerGrain = client.GetGrain<IEventualCustomerGrain>(customerGrainID);
                        tasks.Add(customerGrain.StartTransaction("Init", input));
                    }
                    else
                    {
                        var guid = Helper.ConvertIntToGuid(districtGrainID);
                        var districtGrain = client.GetGrain<IDistrictGrain>(guid);
                        //tasks.Add(districtGrain.StartTransaction("Init", input, new List<GrainID> { new GrainID(guid, districtGrainName) }));
                        tasks.Add(districtGrain.StartTransaction("Init", input));

                        guid = Helper.ConvertIntToGuid(customerGrainID);
                        var customerGrain = client.GetGrain<ICustomerGrain>(guid);
                        //tasks.Add(customerGrain.StartTransaction("Init", input, new List<GrainID> { new GrainID(guid, customerGrainName) }));
                        tasks.Add(customerGrain.StartTransaction("Init", input));
                    }

                    if (tasks.Count == batchSize)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {grainsInThisSilo[districtGrainName].Count} DistrictGrain and {grainsInThisSilo[customerGrainName].Count} CustomerGrain, it tasks {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s. ");

            // STEP 5: load StockGrain
            tasks.Clear();
            for (var W_ID = minWarehouseID; W_ID < minWarehouseID + Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int i = 0; i < Constants.NUM_StockGrain_PER_W; i++)
                {
                    var stockGrainID = W_ID * TPCCManager.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + i;
                    Debug.Assert(grainsInThisSilo[stockGrainName].Contains(stockGrainID));

                    var input = new Tuple<int, int>(W_ID, i);
                    if (eventual)
                    {
                        var grain = client.GetGrain<IEventualStockGrain>(stockGrainID);
                        tasks.Add(grain.StartTransaction("Init", input));
                    }
                    else
                    {
                        var guid = Helper.ConvertIntToGuid(stockGrainID);
                        var grain = client.GetGrain<IStockGrain>(guid);
                        //tasks.Add(grain.StartTransaction("Init", input, new List<GrainID> { new GrainID(guid, stockGrainName) }));
                        tasks.Add(grain.StartTransaction("Init", input));
                    }

                    if (tasks.Count == batchSize)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {grainsInThisSilo[stockGrainName].Count} StockGrain, it tasks {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s. ");

            // STEP 6: load OrderGrain
            tasks.Clear();
            for (var W_ID = minWarehouseID; W_ID < minWarehouseID + Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < TPCCManager.NUM_OrderGrain_PER_D; i++)
                    {
                        var orderGrainID = W_ID * TPCCManager.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * TPCCManager.NUM_OrderGrain_PER_D + i;
                        Debug.Assert(grainsInThisSilo[orderGrainName].Contains(orderGrainID));
                        
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (eventual)
                        {
                            var grain = client.GetGrain<IEventualOrderGrain>(orderGrainID);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        else
                        {
                            var guid = Helper.ConvertIntToGuid(orderGrainID);
                            var grain = client.GetGrain<IOrderGrain>(guid);
                            //tasks.Add(grain.StartTransaction("Init", input, new List<GrainID> { new GrainID(guid, orderGrainName) }));
                            tasks.Add(grain.StartTransaction("Init", input));
                        }

                        if (tasks.Count == batchSize)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
            }
            await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {grainsInThisSilo[orderGrainName].Count} OrderGrain, it tasks {Helper.ChangeFormat((DateTime.Now - start).TotalSeconds, 2)}s. ");

            threadFinishLoadingGrain.Signal();
        }
    }
}
