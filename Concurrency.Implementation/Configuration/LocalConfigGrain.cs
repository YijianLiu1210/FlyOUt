using Orleans;
using Utilities;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;
using System;
using Concurrency.Interface.GrainPlacement;
using StackExchange.Redis;
using System.Collections.Generic;
using Orleans.Concurrency;

namespace Concurrency.Implementation.Configuration
{
    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.LocalConfig)]
    public class LocalConfigGrain : Grain, ILocalConfigGrain
    {
        readonly ILoggingProtocol log;  // this logger group is only accessible within this silo host
        readonly IGrainPlacementCache grainPlacementCache;
        readonly IDatabase siloInfo_db;
        readonly IDatabase grainPlacement_db;

        bool hierarchicalCoord;

        public LocalConfigGrain(
            ILoggingProtocol log, 
            IGrainPlacementCache grainPlacementCache, 
            IConnectionMultiplexer redis)   // dependency injection
        {
            Console.WriteLine($"LocalConfigGrain is activated");
            this.log = log;
            this.grainPlacementCache = grainPlacementCache;
            siloInfo_db = redis.GetDatabase(Constants.Redis_SiloInfo);
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);
        }

        public Task Init(bool hierarchicalCoord)
        {
            this.hierarchicalCoord = hierarchicalCoord;
            grainPlacementCache.SetHierarchicalCoord(hierarchicalCoord);
            return Task.CompletedTask;
        }

        public Task UpdateUserGrainInfoInCache(Guid grainID, string siloAddress)
        {
            grainPlacementCache.UpdateUserGrainInfo(grainID, siloAddress);
            return Task.CompletedTask;
        }

        public Task PrepareCache(bool isGrainMigrationExp)
        {
            grainPlacementCache.PrepareCache(isGrainMigrationExp, RuntimeIdentity);
            return Task.CompletedTask;
        }

        public async Task CheckGC()
        {
            if (hierarchicalCoord)
            {
                // checkGC for all local coordinators in this silo if have any
                var localCoords = grainPlacementCache.GetAllCoordsInSilo(RuntimeIdentity);
                foreach (var guid in localCoords)
                {
                    var coord = GrainFactory.GetGrain<ILocalCoordGrain>(guid);
                    await coord.CheckGC();
                }
            }

            var localPMs = grainPlacementCache.GetAllPMInSilo(RuntimeIdentity);
            foreach (var guid in localPMs)
            {
                var pm = GrainFactory.GetGrain<IGrainPlacementManager>(guid);
                await pm.CheckGC();
            }
        }

        public async Task ConfigLogging(bool isLoggingEnabled, int siloID, int numLocalSilo) => await log.Init(isLoggingEnabled, false, siloID, numLocalSilo, hierarchicalCoord);
        
        public async Task ConfigLocalCoordinator(int siloID, bool optimizeCommit)
        {
            var guids = new List<Guid>();
            // Generate IDs for all local coordinators and write the placement info into redis
            for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
            {
                var guid = Helper.ConvertIntToGuid(i + siloID * Constants.numLocalCoordPerSilo);
                await grainPlacement_db.StringSetAsync(Constants.CoordIDPrefix + guid.ToString(), RuntimeIdentity);
                await siloInfo_db.ListRightPushAsync(Constants.CoordInfoPrefix + RuntimeIdentity, guid.ToString());
                guids.Add(guid);
            }

            var tasks = new List<Task>();
            foreach (var guid in guids)
            {
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(guid);
                tasks.Add(coord.Init(optimizeCommit));
            }
            await Task.WhenAll(tasks);

            // Inject the token to one of the local coordinators
            var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(guids[0]);
            _ = localCoord.PassToken(new LocalToken());
        }

        public async Task ConfigPlacementManager(int numLocalSilo, int siloID)
        {
            for (int i = 0; i < Constants.numGrainPlacementManagerPerSilo; i++)
            {
                var guid = Helper.ConvertIntToGuid(i + siloID * Constants.numGrainPlacementManagerPerSilo);
                await grainPlacement_db.StringSetAsync("PlacementManager-" + guid.ToString(), RuntimeIdentity);
                await siloInfo_db.ListRightPushAsync("PlacementManagerList-" + RuntimeIdentity, guid.ToString());
                await siloInfo_db.ListRightPushAsync("PlacementManagerList", guid.ToString());
                var manager = GrainFactory.GetGrain<IGrainPlacementManager>(guid);
                await manager.Init(numLocalSilo);
            }
        }

        public async Task ConfigMigrationWorker(int siloID)
        {
            // grainID --> MW ID must be static, every time we want to migrate grain i, we should always let the same worker do it
            for (int i = 0; i < Constants.numGrainMigrationWorkerPerSilo; i++)
            {
                var guid = Helper.ConvertIntToGuid(i + siloID * Constants.numGrainMigrationWorkerPerSilo);
                await grainPlacement_db.StringSetAsync("MigrationWorker-" + guid.ToString(), RuntimeIdentity);
                await siloInfo_db.ListRightPushAsync("MigrationWorkerList-" + RuntimeIdentity, guid.ToString());
                await siloInfo_db.ListRightPushAsync("MigrationWorkerList", guid.ToString());
                var worker = GrainFactory.GetGrain<IGrainMigrationWorker>(guid);
                await worker.Init();
            }
        }
    }
}