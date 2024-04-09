using Concurrency.Interface.Configuration;
using Concurrency.Interface.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Orleans;
using Orleans.Concurrency;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.GrainPlacement
{
    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.MigrationWorker)]
    public class GrainMigrationWorker : Grain, IGrainMigrationWorker
    {
        Guid myID;
        HashSet<GrainID> grainToMigrate;
        readonly IDatabase grainPlacement_db;
        readonly IGrainPlacementCache grainPlacementCache;

        public GrainMigrationWorker(IConnectionMultiplexer redis, IGrainPlacementCache grainPlacementCache)
        {
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);
            this.grainPlacementCache = grainPlacementCache;
        }

        public Task CheckGC()
        {
            if (grainToMigrate.Count != 0) Console.WriteLine($"GrainMigrationWorker: grainToMigrate.Count = {grainToMigrate.Count}");
            return Task.CompletedTask;
        }

        public Task Init()
        {
            myID = this.GetPrimaryKey();

            grainToMigrate = new HashSet<GrainID>();
            return Task.CompletedTask;
        }

        public async Task<GrainMigrationRequestResult> MigrateGrain(GrainID grainID, string targetSilo)
        {
            // check if this MigrationWorker is the correct one to handle this request
            var mw = grainPlacementCache.GetMigrationWorker(grainID.id);
            if (mw == myID) return await DoMigration(grainID, targetSilo);
            else
            {
                var worker = GrainFactory.GetGrain<IGrainMigrationWorker>(mw);
                return await worker.DoMigration(grainID, targetSilo);
            }
        }

        public async Task<GrainMigrationRequestResult> DoMigration(GrainID grainID, string targetSilo)
        {
            var start = DateTime.Now;
            // can only allow one migration request be handled for each actor
            if (grainToMigrate.Contains(grainID)) 
                throw new SnapperGrainMigrationException($"Grain {Helper.ConvertGuidToInt(grainID.id)} is under migration, try again later. ");
            var old_silo = grainPlacementCache.GetSilo(grainID.id);
            if (old_silo == targetSilo) throw new SnapperGrainMigrationException($"Grain {Helper.ConvertGuidToInt(grainID.id)} is already in the target silo {targetSilo}. ");
            grainToMigrate.Add(grainID);
            
            var res = new GrainMigrationRequestResult();

            // tell the actor to stop receiving new transaction requests
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.id, grainID.className);
            await grain.StartMigration();
            res.informGrainTime = (DateTime.Now - start).TotalMilliseconds;

            // freeze the grain in all GrainPlacementManager
            start = DateTime.Now;
            var allPMs = grainPlacementCache.GetAllPM();
            var tasks = new List<Task<Tuple<long, long>>>();
            foreach (var pm in allPMs)
            {
                var manager = GrainFactory.GetGrain<IGrainPlacementManager>(pm);
                tasks.Add(manager.FreezeGrain(grainID));
            }
            await Task.WhenAll(tasks);
            res.freezeGrainTime = (DateTime.Now - start).TotalMilliseconds;

            start = DateTime.Now;
            long localBid = -1;
            long globalBid = -1;
            foreach (var t in tasks)
            {
                // notice: the ltid collected from other silos must be -1
                if (t.Result.Item1 > localBid) localBid = t.Result.Item1;
                if (t.Result.Item2 > globalBid) globalBid = t.Result.Item2;
            }

            // wait until the grain has committed all batches and finished all ACTs
            var result = await grain.PrepareDeactivation(localBid, globalBid);
            var lastCommittedState = result.Item4;
            res.waitTxnCommitTime = result.Item1;
            res.getCommittedStateTime = result.Item2;
            res.deserializeStateTime = result.Item3;
            res.prepareDeactivateTime = (DateTime.Now - start).TotalMilliseconds;
            
            // update grain placement info (redis + cached info in each silo)
            start = DateTime.Now;
            await grainPlacement_db.HashSetAsync(Constants.GrainIDPrefix + grainID.ToString(), "SiloAddress", targetSilo);
            res.updateRedisTime = (DateTime.Now - start).TotalMilliseconds;

            start = DateTime.Now;
            var globalConfigGrain = GrainFactory.GetGrain<IGlobalConfigGrain>("GlobalConfigGrain");
            await globalConfigGrain.UpdateUserGrainInfoInCache(grainID.id, targetSilo);
            res.updateCacheTime = (DateTime.Now - start).TotalMilliseconds;

            // de-activate the grain
            start = DateTime.Now;
            await grain.DeactivateGrain();
            res.deactivateTime = (DateTime.Now - start).TotalMilliseconds;

            // activate the grain
            start = DateTime.Now;
            await grain.ActivateGrain(lastCommittedState);
            res.activateTime = (DateTime.Now - start).TotalMilliseconds;

            // unfreeze the grain
            start = DateTime.Now;
            var new_tasks = new List<Task>();
            foreach (var pm in allPMs)
            {
                var manager = GrainFactory.GetGrain<IGrainPlacementManager>(pm);
                new_tasks.Add(manager.UnFreezeGrain(grainID));
            }
            await Task.WhenAll(new_tasks);
            grainToMigrate.Remove(grainID);
            res.unfreezeTime = (DateTime.Now - start).TotalMilliseconds;
            return res;
        }
    }
}