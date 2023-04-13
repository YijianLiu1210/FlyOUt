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
        readonly IConnectionMultiplexer redis;
        readonly IDatabase grainPlacement_db;
        readonly IDatabase siloInfo_db;

        public ServerConnector(int numLocalSilo, ImplementationType implementationType, bool isLoggingEnabled, string redis_ConnectionString,
            bool hierarchicalCoord, bool optimizeCommit)
        {
            this.numLocalSilo = numLocalSilo;
            this.implementationType = implementationType;
            this.isLoggingEnabled = isLoggingEnabled;
            this.hierarchicalCoord = hierarchicalCoord;
            this.optimizeCommit = optimizeCommit;
            redis = ConnectionMultiplexer.Connect(new ConfigurationOptions { EndPoints = { redis_ConnectionString }, AllowAdmin = true });
            siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);

            preparationDone = false;
            PrepareEnv(redis_ConnectionString);
            while(preparationDone == false) Thread.Sleep(100);
        }

        async void PrepareEnv(string redis_ConnectionString)
        {
            // flush all data stored in Redis cluster
            var server = redis.GetServer(redis_ConnectionString);
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
            if (Constants.benchmark == BenchmarkType.SMALLBANK) 
                LoadSmallBankGrains(isGrainMigrationExp);
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
                globalConfigGrain = client.GetGrain<IGlobalConfigGrain>("GlobalConfigGrain");
                await globalConfigGrain.ConfigGlobalEnv(numLocalSilo, isLoggingEnabled, hierarchicalCoord, optimizeCommit);
                Console.WriteLine($"Spawned the global configuration grain.");
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
                await globalConfigGrain.PrepareCache(isGrainMigrationExp);
                Console.WriteLine("GrainPlacementCache is prepared for all local silos");
            }
            cachePrepared = true;
        }

        public void CheckGC(bool isGrainMigrationExp)
        {
            if (implementationType != ImplementationType.SNAPPER) return;
            if (Constants.benchmark != BenchmarkType.SMALLBANK) return;

            checkGCFinish = false;
            CheckGCAsync(isGrainMigrationExp);
            while (!checkGCFinish) Thread.Sleep(100);
        }

        async void CheckGCAsync(bool isGrainMigrationExp)
        {
            // check all global & local coordinators
            var tasks = new List<Task>();
            await globalConfigGrain.CheckGC();

            var numSiloWithGrains = isGrainMigrationExp ? numLocalSilo / 2 : numLocalSilo;
            // check all transactional grains
            for (int i = 0; i < Constants.numGrainPerLocalSilo * numSiloWithGrains; i++)
            {
                var grain = client.GetGrain<ISnapperTransactionalAccountGrain>(Helper.ConvertIntToGuid(i));
                tasks.Add(grain.CheckGC());
            }

            await Task.WhenAll(tasks);
            checkGCFinish = true;
        }

        void LoadSmallBankGrains(bool isGrainMigrationExp)
        {
            registeredSilo = Helper.GetLocalSiloList(siloInfo_db);
            Debug.Assert(registeredSilo.Count == numLocalSilo);

            // spawn multiple threads to load grains
            var numSiloWithGrains = isGrainMigrationExp ? numLocalSilo / 2 : numLocalSilo;
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
            var grainsInThisSilo = Helper.GetGrainsOfSilo(threadID);

            // write the initial grain placement info to redis
            var start = DateTime.Now;
            var tasks = new List<Task>();
            foreach (var id in grainsInThisSilo)
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
                foreach (var id in grainsInThisSilo)
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
            foreach (var id in grainsInThisSilo)
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
    }
}
