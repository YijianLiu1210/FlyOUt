using System;
using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Orleans.Concurrency;
using Concurrency.Interface.GrainPlacement;
using System.Diagnostics;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.GlobalCoord)]
    public class GlobalCoordGrain : Grain, IGlobalCoordGrain
    {
        // coord basic info
        Guid myID;
        readonly ILoggingProtocol log;
        IGlobalCoordGrain neighborCoord;
        readonly IGrainPlacementCache grainPlacementCache;

        // PACT
        DetTxnProcessor detTxnProcessor;
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<string, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        Dictionary<long, Dictionary<string, Guid>> coordPerBatchPerSilo;   // <global bid, siloID, chosen local Coord ID>

        // ACT
        NonDetTxnProcessor nonDetTxnProcessor;

        DateTime timeOfBatchGeneration;
        double batchSizeInMSecs;

        public Task CheckGC()
        {
            detTxnProcessor.CheckGC();
            nonDetTxnProcessor.CheckGC();
            if (expectedAcksPerBatch.Count != 0) Console.WriteLine($"GlobalCoord: expectedAcksPerBatch.Count = {expectedAcksPerBatch.Count}");
            if (bidToSubBatches.Count != 0) Console.WriteLine($"GlobalCoord: batchSchedulePerSilo.Count = {bidToSubBatches.Count}");
            if (coordPerBatchPerSilo.Count != 0) Console.WriteLine($"GlobalCoord: coordPerBatchPerSilo.Count = {coordPerBatchPerSilo.Count}");
            return Task.CompletedTask;
        }

        public GlobalCoordGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementCache)
        {
            this.log = log;
            this.grainPlacementCache = grainPlacementCache;
        }

        public Task SetBatchSize(int numLocalSilo, int batchSizeInMSecsBasic)
        {
            batchSizeInMSecs = batchSizeInMSecsBasic;
            for (int i = numLocalSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeedForGlobalBatchSize;
            Console.WriteLine($"GlobalCoord {Helper.ConvertGuidToInt(myID)}: batch size = {Helper.ChangeFormat(batchSizeInMSecs, 0)}ms");
            return Task.CompletedTask;
        }

        public Task Init()
        {
            myID = this.GetPrimaryKey();
            expectedAcksPerBatch = new Dictionary<long, int>();
            bidToSubBatches = new Dictionary<long, Dictionary<string, SubBatch>>();
            coordPerBatchPerSilo = new Dictionary<long, Dictionary<string, Guid>>();
            nonDetTxnProcessor = new NonDetTxnProcessor(myID);
            nonDetTxnProcessor.Init();
            detTxnProcessor = new DetTxnProcessor(
                myID,
                GrainFactory,
                grainPlacementCache,
                expectedAcksPerBatch,
                bidToSubBatches,
                coordPerBatchPerSilo);
            detTxnProcessor.Init();

            var neighborID = grainPlacementCache.GetCoordNeighborID(RuntimeIdentity, myID);
            neighborCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(neighborID);
            timeOfBatchGeneration = DateTime.Now;
            Console.WriteLine($"GlobalCoord {Helper.ConvertGuidToInt(myID)}: neighbor = {Helper.ConvertGuidToInt(neighborID)}");
            return Task.CompletedTask;
        }

        public Task<long> GetHighestCommittedGlobalBid() => Task.FromResult(detTxnProcessor.highestCommittedBid);

        public async Task<Tuple<long, long, Dictionary<string, Guid>>> NewGlobalPACT(List<string> siloList)
        {
            var id = await detTxnProcessor.NewDet(siloList);
            return new Tuple<long, long, Dictionary<string, Guid>>(id.Item1, id.Item2, coordPerBatchPerSilo[id.Item1]);
        }

        public async Task<Tuple<long, long>> NewACT() => new Tuple<long, long>(await nonDetTxnProcessor.NewNonDet(), detTxnProcessor.highestCommittedBid);
        
        public async Task PassToken(BasicToken token)
        {
            try
            {
                long curBatchID = -1;
                var elapsedTime = (DateTime.Now - timeOfBatchGeneration).TotalMilliseconds;
                if (elapsedTime >= batchSizeInMSecs)
                {
                    curBatchID = detTxnProcessor.GenerateBatch(token);
                    if (curBatchID != -1) timeOfBatchGeneration = DateTime.Now;
                }

                nonDetTxnProcessor.EmitNonDetTransactions(token);

                if (detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
                    token.highestCommittedBid = detTxnProcessor.highestCommittedBid;
                else detTxnProcessor.highestCommittedBid = token.highestCommittedBid;

                _ = neighborCoord.PassToken(token);
                if (curBatchID != -1) await EmitBatch(curBatchID);
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.Message} {e.StackTrace}");
                Debug.Assert(false);
            }
        }

        async Task EmitBatch(long bid)
        {
            var coords = coordPerBatchPerSilo[bid];
            var curScheduleMap = bidToSubBatches[bid];

            if (log.IsLoggingEnabled()) await log.GlobalBatchInfo(myID, bid, new HashSet<Guid>(coords.Values));

            var tasks = new List<Task>();
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
                item.Value.highestCommittedBid = detTxnProcessor.highestCommittedBid;
                tasks.Add(dest.ReceiveBatchSchedule(item.Value));
            }
            await Task.WhenAll(tasks);
        }

        public async Task AckBatchCompletion(long bid)
        {
            // count down the number of expected ACKs from different silos
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] != 0) return;

            await detTxnProcessor.WaitPrevBatchToCommit(bid);
            detTxnProcessor.AckBatchCommit(bid);

            // send ACKs to local coordinators
            var tasks = new List<Task>();
            var curScheduleMap = bidToSubBatches[bid];
            var coords = coordPerBatchPerSilo[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
                tasks.Add(dest.AckGlobalBatchCommit(bid));
            }
            await Task.WhenAll(tasks);

            // garbage collection
            bidToSubBatches.Remove(bid);
            coordPerBatchPerSilo.Remove(bid);
            expectedAcksPerBatch.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid) => await detTxnProcessor.WaitBatchCommit(bid);
    }
}