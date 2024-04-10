using System;
using Utilities;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.Logging;
using Orleans;
using MessagePack;
using System.Diagnostics;
using Concurrency.Interface.GrainPlacement;
using System.Linq;

namespace Concurrency.Implementation.TransactionExecution.Nondeterministic
{
    // cannot support hybrid commit for Timestamp-based concurrency control
    public class NonDetCommitter<TState> where TState : ICloneable, IPrintable
    {
        readonly GrainID myID;
        readonly IGrainPlacementCache grainPlacementInfo;
        readonly Dictionary<long, GrainID> coordinatorMap;         // <global ACT tid, the grain ID who starts the ACT>
        readonly ILoggingProtocol log;
        readonly IGrainFactory myGrainFactory;
        readonly ITransactionalState<TState> state;
        readonly CommitInfo commitInfo;
        readonly Dictionary<DateTime, byte[]> lastPreparedState;

        public NonDetCommitter(
            GrainID myID,
            Dictionary<long, GrainID> coordinatorMap,
            ITransactionalState<TState> state, 
            ILoggingProtocol log,
            IGrainPlacementCache grainPlacementInfo,
            IGrainFactory myGrainFactory,
            CommitInfo commitInfo,
            Dictionary<DateTime, byte[]> lastPreparedState)
        {
            this.myID = myID;
            this.state = state;
            this.log = log;
            this.myGrainFactory = myGrainFactory;
            this.coordinatorMap = coordinatorMap;
            this.grainPlacementInfo = grainPlacementInfo;
            this.commitInfo = commitInfo;
            this.lastPreparedState = lastPreparedState;
        }

        public void CheckGC() { }

        // serializable or not, sure or not sure
        public Tuple<bool, bool> CheckSerializability(NonDetScheduleInfo globalScheduleInfo, Dictionary<string, NonDetScheduleInfo> scheduleInfoPerSilo)
        {
            // check global bid
            var globalRes = Check("", globalScheduleInfo.maxBeforeBid, globalScheduleInfo.minAfterBid, globalScheduleInfo.isAfterComplete);
            var isSerializable = globalRes.Item1;
            var isSure = globalRes.Item2;
            
            // check local bid in each silo
            foreach (var infoPerSilo in scheduleInfoPerSilo)
            {
                var info = infoPerSilo.Value;
                var res = Check(infoPerSilo.Key, info.maxBeforeBid, info.minAfterBid, info.isAfterComplete);
                isSerializable &= res.Item1;
                isSure &= res.Item2;
            }

            return new Tuple<bool, bool>(isSerializable, isSure);
        }

        // serializable or not, sure or not sure
        Tuple<bool, bool> Check(string siloAddress, long maxBeforeBid, long minAfterBid, bool isAfterComplete)
        {
            if (maxBeforeBid == -1) return new Tuple<bool, bool>(true, true);

            // if the maxBeforeBid has already committed, pass
            if (siloAddress == "") 
                if (maxBeforeBid <= commitInfo.highestCommittedGlobalBid) return new Tuple<bool, bool>(true, true);
            else if (commitInfo.highestCommittedLocalBidPerSilo.ContainsKey(siloAddress))
                if (maxBeforeBid <= commitInfo.highestCommittedLocalBidPerSilo[siloAddress]) return new Tuple<bool, bool>(true, true);

            if (isAfterComplete && maxBeforeBid < minAfterBid) return new Tuple<bool, bool>(true, true);
            if (maxBeforeBid >= minAfterBid) return new Tuple<bool, bool>(false, true);
            return new Tuple<bool, bool>(false, false);
        }

        public async Task<bool> CoordPrepare(long tid, Dictionary<GrainID, OpOnGrain> grainOpInfo)
        {
            if (log.IsLoggingEnabled()) await log.CoordPrepare(myID, tid, grainOpInfo.Keys.ToHashSet());

            var prepareTask = new List<Task<bool>>();
            foreach (var item in grainOpInfo)
            {
                if (item.Value.isNoOp) continue;  
                // reader grain needs to Prepare, because it should release the read lock
                // writer grain needs to Prepare, because it must persist the grain state
                var grain = myGrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.id, item.Key.className);
                var t = grain.Prepare(tid, item.Value.isReadonly);
                prepareTask.Add(t);
            }
            var task = Task.WhenAll(prepareTask);
            await Task.WhenAny(task, Task.Delay(Constants.lostMsgTimeout));

            // if the Prepare message is lost because the target grain is under migration
            if (task.IsCompleted == false) return false;

            foreach (var vote in prepareTask)
                if (vote.Result == false) return false;

            return true;
        }

        public async Task CoordCommit(long tid, NonDetFuncResult funcResult)
        {
            if (log.IsLoggingEnabled()) await log.CoordCommit(myID, tid);

            var tasks = new List<Task>();
            foreach (var item in funcResult.grainOpInfo)
            {
                // if the grain has only been read or it's no-op, no need 2nd phase
                if (item.Value.isNoOp || item.Value.isReadonly) continue;

                // writer grain needs 2nd phase, because it can only release the write lock in the 2nd phase
                var grain = myGrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.id, item.Key.className);
                var siloAddress = grainPlacementInfo.GetSilo(item.Key.id);
                if (funcResult.scheduleInfoPerSilo.ContainsKey(siloAddress) == false)
                    throw new Exception($"CoordCommit: funcResult does not contain silo address {siloAddress}");
                tasks.Add(grain.Commit(tid, funcResult.scheduleInfoPerSilo[siloAddress].maxBeforeBid, funcResult.globalScheduleInfo.maxBeforeBid));
            }
            await Task.WhenAll(tasks);
        }

        // the ACT aborted due to RW conflicts will come to Abort phase directly (without Prepare phase)
        public async Task CoordAbort(long tid, Dictionary<GrainID, OpOnGrain> grainOpInfo, bool isPrepared)
        {
            var tasks = new List<Task>();
            // Presume Abort: we do not write abort logs, when recovering, if no log record is found, we assume the transaction was aborted
            foreach (var item in grainOpInfo)
            {
                // if the grain does no-op, no need 2nd phase
                if (item.Value.isNoOp) continue;
                if (isPrepared && item.Value.isReadonly) continue;
                // reader grain which has not been prepared needs to do Abort, because it needs to do garbage collection
                // writer grain needs 2nd phase, because it can only release the write lock in the 2nd phase
                var grain = myGrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.id, item.Key.className);
                tasks.Add(grain.Abort(tid));
            }
            await Task.WhenAll(tasks);
        }

        public async Task<bool> Prepare(long tid, bool isReader)
        {
            var vote = await state.Prepare(tid, isReader);
            if (vote && !isReader)
            {
                var timestamp = DateTime.Now;
                var s = state.GetPreparedState(tid);
                if (Constants.benchmark == BenchmarkType.SMALLBANK) Debug.Assert(s.PrintState() == myID.id.ToString());
                var data = MessagePackSerializer.Serialize(s);
                lastPreparedState.Clear();
                lastPreparedState.Add(timestamp, data);
                if (log.IsLoggingEnabled()) await log.Prepare(myID, tid, coordinatorMap[tid], data, timestamp);
            }
            return vote;
        }

        public async Task Commit(long tid)
        {
            state.Commit(tid);
            if (log.IsLoggingEnabled()) await log.Commit(myID, tid);
        }

        public void Abort(long tid) => state.Abort(tid);
    }
}