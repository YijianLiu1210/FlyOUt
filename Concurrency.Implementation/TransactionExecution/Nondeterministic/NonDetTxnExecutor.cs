using Concurrency.Interface.Coordinator;
using Concurrency.Interface.TransactionExecution;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution
{
    public class NonDetTxnExecutor<TState> where TState : ICloneable, IPrintable
    {
        readonly Guid myID;
        readonly string siloAddress;
        readonly ILocalCoordGrain myLocalCoord;
        readonly IGlobalCoordGrain myGlobalCoord;
        readonly bool hierarchicalCoord;

        readonly ITransactionalState<TState> state;
        readonly TransactionScheduler myScheduler;
        readonly CommitInfo commitInfo;

        // ACT execution
        Dictionary<long, NonDetFuncResult> nonDetFuncResults;    // key: global ACT tid

        // hybrid execution
        // NOTICE: if every PACT awaits every grain call, then if there is no deadlock, there is also no global sesrializability issue
        // In Snapper, every ACT's grain call must await, because Snapper needs to collect grain access info
        // In Snapper, if we allow PACT to not await every grain call, we must do serailizability check for every ACT
        long maxBeforeLocalBidOnGrain;                           // maxBeforeBid of the current executing / latest executed ACT
        long maxBeforeGlobalBidOnGrain;
        
        public void CheckGC()
        {
            if (nonDetFuncResults.Count != 0) Console.WriteLine($"NonDetTxnExecutor: nonDetFuncResults.Count = {nonDetFuncResults.Count}");
        }

        public NonDetTxnExecutor(
            Guid myID,
            string siloAddress,
            ILocalCoordGrain myLocalCoord,
            IGlobalCoordGrain myGlobalCoord,
            bool hierarchicalCoord,
            TransactionScheduler myScheduler, 
            ITransactionalState<TState> state,
            CommitInfo commitInfo)
        {
            this.myID = myID;
            this.siloAddress = siloAddress;
            this.myLocalCoord = myLocalCoord;
            this.myGlobalCoord = myGlobalCoord;
            this.hierarchicalCoord = hierarchicalCoord;
            this.myScheduler = myScheduler;
            this.state = state;

            nonDetFuncResults = new Dictionary<long, NonDetFuncResult>();
            maxBeforeLocalBidOnGrain = -1;
            maxBeforeGlobalBidOnGrain = -1;
            this.commitInfo = commitInfo;
        }

        public async Task<TransactionContext> GetNonDetContext()
        {
            long tid;
            if (hierarchicalCoord)
            {
                var info = await myGlobalCoord.NewACT();
                tid = info.Item1;
                commitInfo.MergeCommitInfoOfSilo("", info.Item2);
            }
            else
            {
                var info = await myLocalCoord.NewACT();
                tid = info.Item1;
                commitInfo.MergeCommitInfoOfSilo("", info.Item2);
                commitInfo.MergeCommitInfoOfSilo(siloAddress, info.Item3);
            }
            return new TransactionContext(tid, myID);
        }

        public async Task<bool> WaitForTurn(long tid)
        {
            // wait for turn to execute
            var t = myScheduler.WaitForTurn(tid);
            await Task.WhenAny(t, Task.Delay(Constants.deadlockTimeout));
            if (t.IsCompleted)
            {
                Debug.Assert(nonDetFuncResults.ContainsKey(tid) == false);
                nonDetFuncResults.Add(tid, new NonDetFuncResult());
            }
            else myScheduler.scheduleInfo.CompleteNonDetTxn(tid);
            return t.IsCompleted;
        }

        public async Task<TState> GetState(long tid, AccessMode mode)
        {
            try
            {
                if (mode == AccessMode.Read)
                {
                    var myState = await state.NonDetRead(tid);
                    nonDetFuncResults[tid].isNoOpOnGrain = false;
                    nonDetFuncResults[tid].isReadOnlyOnGrain = true;
                    return myState;
                }
                else
                {
                    var myState = await state.NonDetReadWrite(tid);
                    nonDetFuncResults[tid].isNoOpOnGrain = false;
                    nonDetFuncResults[tid].isReadOnlyOnGrain = false;
                    return myState;
                }
            }
            catch (SnapperDeadlockException)
            {
                nonDetFuncResults[tid].exception = true;
                Debug.Assert(nonDetFuncResults[tid].isNoOpOnGrain && nonDetFuncResults[tid].isReadOnlyOnGrain);
                throw;
            }
        }

        public async Task<TransactionResult> CallGrain(TransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            Tuple<NonDetFuncResult, DateTime> funcResult;
            var t = grain.ExecuteNonDet(call, cxt);
            await Task.WhenAny(t, Task.Delay(Constants.lostMsgTimeout));
            if (t.IsCompleted) funcResult = t.Result;
            else
            {
                // if the call is lost due to grain migration, this will be a timeout exception
                var res = new NonDetFuncResult();
                res.Exp_GrainMigration = true;
                res.exception = true;
                funcResult = new Tuple<NonDetFuncResult, DateTime>(res, DateTime.Now);
            }

            nonDetFuncResults[cxt.globalTid].MergeFuncResult(funcResult.Item1);
            commitInfo.MergeCommitInfo(funcResult.Item1.commitInfo);
            return new TransactionResult(funcResult.Item1.resultObj);
        }

        // Update the metadata of the execution results, including accessed grains, before/after set, etc.
        public NonDetFuncResult UpdateExecutionResult(long tid)
        {
            var res = nonDetFuncResults[tid];
            
            if (res.grainOpInfo.ContainsKey(myID) == false)
                res.grainOpInfo.Add(myID, new OpOnGrain(res.isNoOpOnGrain, res.isReadOnlyOnGrain));

            NonDetScheduleInfo localInfo;
            NonDetScheduleInfo globalInfo;
            myScheduler.scheduleInfo.GetBeforeAfterInfo(tid, out localInfo, out globalInfo);
            localInfo.maxBeforeBid = Math.Max(localInfo.maxBeforeBid, maxBeforeLocalBidOnGrain);
            globalInfo.maxBeforeBid = Math.Max(globalInfo.maxBeforeBid, maxBeforeGlobalBidOnGrain);
            res.MergeBeforeAfterLocalInfo(localInfo, siloAddress);
            res.MergeBeforeAfterGlobalInfo(globalInfo);
            res.commitInfo.MergeCommitInfo(commitInfo);
            return res;
        }

        public void Commit(long maxBeforeLocalBid, long maxBeforeGlobalBid)
        {
            Debug.Assert(maxBeforeLocalBidOnGrain <= maxBeforeLocalBid && maxBeforeGlobalBidOnGrain <= maxBeforeGlobalBid);
            maxBeforeLocalBidOnGrain = maxBeforeLocalBid;
            maxBeforeGlobalBidOnGrain = maxBeforeGlobalBid;
        }

        public void CleanUp(long tid) => nonDetFuncResults.Remove(tid);
    }
}