using System;
using Utilities;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Coordinator;
using Orleans;
using System.Diagnostics;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.Logging;
using MessagePack;
using Concurrency.Interface.GrainPlacement;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DetTxnExecutor<TState> where TState : ICloneable, IPrintable
    {
        // grain basic info
        readonly GrainID myID;
        readonly string siloAddress;
        readonly IGrainFactory myGrainFactory;

        // transaction execution
        readonly TransactionScheduler myScheduler;
        readonly ITransactionalState<TState> state;
        readonly ILoggingProtocol log;
        readonly IGrainPlacementManager myGrainPlacementManager;
        readonly CommitInfo commitInfo;
        
        // PACT execution
        Dictionary<long, TaskCompletionSource> localBtchInfoPromise;           // key: local bid, use to check if the SubBatch has arrived or not
        Dictionary<long, BasicFuncResult> detFuncResults;                      // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times
        SortedDictionary<long, Tuple<DateTime, byte[]>> statePerBatch;         // <localBid, timestamp, state>

        // only for global PACT
        Dictionary<long, long> globalBidToLocalBid;
        Dictionary<long, Dictionary<long, long>> globalTidToLocalTidPerBatch;  // key: global bid, <global tid, local tid>
        Dictionary<long, TaskCompletionSource> globalBtchInfoPromise;          // key: global bid, use to check if the SubBatch has arrived or not

        public void CheckGC()
        {
            if (localBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: localBtchInfoPromise.Count = {localBtchInfoPromise.Count}");
            if (detFuncResults.Count != 0) Console.WriteLine($"DetTxnExecutor: detFuncResults.Count = {detFuncResults.Count}");
            if (globalBidToLocalBid.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBidToLocalBid.Count = {globalBidToLocalBid.Count}");
            if (globalTidToLocalTidPerBatch.Count != 0) Console.WriteLine($"DetTxnExecutor: globalTidToLocalTidPerBatch.Count = {globalTidToLocalTidPerBatch.Count}");
            if (globalBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBtchInfoPromise.Count = {globalBtchInfoPromise.Count}");
        }

        public DetTxnExecutor(
            GrainID myID,
            string siloAddress,
            IGrainFactory myGrainFactory,
            TransactionScheduler myScheduler,
            ITransactionalState<TState> state,
            ILoggingProtocol log,
            IGrainPlacementManager myGrainPlacementManager,
            CommitInfo commitInfo,
            SortedDictionary<long, Tuple<DateTime, byte[]>> statePerBatch)
        {
            this.myID = myID;
            this.siloAddress = siloAddress;
            this.myGrainFactory = myGrainFactory;
            this.myScheduler = myScheduler;
            this.state = state;
            this.log = log;
            this.myGrainPlacementManager = myGrainPlacementManager;
            this.commitInfo = commitInfo;
            this.statePerBatch = statePerBatch;

            localBtchInfoPromise = new Dictionary<long, TaskCompletionSource>();
            detFuncResults = new Dictionary<long, BasicFuncResult>();
            globalBidToLocalBid = new Dictionary<long, long>();
            globalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            globalBtchInfoPromise = new Dictionary<long, TaskCompletionSource>();
        }

        public async Task<MyTransactionContext> GetDetContext(List<GrainID>  grainAccessInfo)
        {
            var info = await myGrainPlacementManager.NewTransaction(grainAccessInfo);
            commitInfo.MergeCommitInfoOfSilo("", info.Item2);
            commitInfo.MergeCommitInfoOfSilo(siloAddress, info.Item3);
            return info.Item1;
        }

        public async Task WaitForTurn(MyTransactionContext cxt)
        {
            // check if it is a global PACT
            if (cxt.globalBid != -1)
            {
                // wait until the SubBatch has arrived this grain
                if (globalBtchInfoPromise.ContainsKey(cxt.globalBid) == false)
                    globalBtchInfoPromise.Add(cxt.globalBid, new TaskCompletionSource());
                await globalBtchInfoPromise[cxt.globalBid].Task;

                // need to map global info to the corresponding local tid and bid
                cxt.localBid = globalBidToLocalBid[cxt.globalBid];
                cxt.localTid = globalTidToLocalTidPerBatch[cxt.globalBid][cxt.globalTid];
            }
            else
            {
                // wait until the SubBatch has arrived this grain
                if (localBtchInfoPromise.ContainsKey(cxt.localBid) == false)
                    localBtchInfoPromise.Add(cxt.localBid, new TaskCompletionSource());
                await localBtchInfoPromise[cxt.localBid].Task;
            }
            
            Debug.Assert(detFuncResults.ContainsKey(cxt.localTid) == false);
            detFuncResults.Add(cxt.localTid, new BasicFuncResult());
            await myScheduler.WaitForTurn(cxt.localBid, cxt.localTid);
        }

        public async Task FinishExecuteDetTxn(MyTransactionContext cxt)
        {
            var tuple = myScheduler.AckComplete(cxt.localBid, cxt.localTid);
            if (tuple.Item1)   // the current batch has completed on this grain
            {
                var coordID = tuple.Item2;

                var timestamp = DateTime.Now;
                var s = state.GetCommittedState();
                if (Constants.benchmark == BenchmarkType.SMALLBANK) Debug.Assert(s.PrintState() == myID.ToString());
                var data = MessagePackSerializer.Serialize(s);
                Debug.Assert(statePerBatch.ContainsKey(cxt.localBid) == false);
                statePerBatch[cxt.localBid] = new Tuple<DateTime, byte[]>(timestamp, data);
                if (log.IsLoggingEnabled()) await log.LocalBatchComplete(myID, cxt.localBid, coordID, data, timestamp);

                myScheduler.scheduleInfo.CompleteDetBatch(cxt.localBid);

                var coord = myGrainFactory.GetGrain<ILocalCoordGrain>(coordID);
                _ = coord.AckBatchCompletion(cxt.localBid);
               
                localBtchInfoPromise.Remove(cxt.localBid);
                if (cxt.globalBid != -1)
                {
                    globalBidToLocalBid.Remove(cxt.globalBid);
                    globalTidToLocalTidPerBatch.Remove(cxt.globalBid);
                    globalBtchInfoPromise.Remove(cxt.globalBid);
                }
            }
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public void BatchArrive(LocalSubBatch batch)
        {
            if (localBtchInfoPromise.ContainsKey(batch.bid) == false)
                localBtchInfoPromise.Add(batch.bid, new TaskCompletionSource());
            localBtchInfoPromise[batch.bid].SetResult();

            // register global info mapping if necessary
            if (batch.globalBid != -1)
            {
                globalBidToLocalBid.Add(batch.globalBid, batch.bid);
                globalTidToLocalTidPerBatch.Add(batch.globalBid, batch.globalTidToLocalTid);

                if (globalBtchInfoPromise.ContainsKey(batch.globalBid) == false)
                    globalBtchInfoPromise.Add(batch.globalBid, new TaskCompletionSource());
                globalBtchInfoPromise[batch.globalBid].SetResult();
            }
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public TState GetState(long tid, AccessMode mode)
        {
            if (mode == AccessMode.Read)
            {
                detFuncResults[tid].isNoOpOnGrain = false;
                detFuncResults[tid].isReadOnlyOnGrain = true;
            }
            else
            {
                detFuncResults[tid].isNoOpOnGrain = false;
                detFuncResults[tid].isReadOnlyOnGrain = false;
            }
            var res = state.GetCommittedState();
            if (Constants.benchmark == BenchmarkType.SMALLBANK) Debug.Assert(res.PrintState() == myID.ToString());
            return res;
        }

        public async Task<TransactionResult> CallGrain(MyTransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            var resultObj = (await grain.ExecuteDet(call, cxt)).Item1;
            return new TransactionResult(resultObj);
        }

        public void CleanUp(long tid) => detFuncResults.Remove(tid);
    }
}