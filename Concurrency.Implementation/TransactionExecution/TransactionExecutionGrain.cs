using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.TransactionExecution.Nondeterministic;
using Orleans.GrainDirectory;
using Concurrency.Interface.GrainPlacement;
using StackExchange.Redis;
using MessagePack;
using System.Linq;

namespace Concurrency.Implementation.TransactionExecution
{
    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.UserGrain)]
    [GrainDirectory(GrainDirectoryName = Constants.GrainDirectoryName)]
    public abstract class TransactionExecutionGrain<TState> : Grain, ITransactionExecutionGrain where TState : ICloneable, IPrintable, new()
    {
        // grain basic info
        Guid myID;
        readonly IGrainPlacementCache grainPlacementCache;
        readonly IDatabase grainPlacement_db;
        bool hierarchicalCoord;

        // transaction execution
        readonly ILoggingProtocol log;
        TransactionScheduler myScheduler;
        ITransactionalState<TState> state;

        // PACT execution
        DetTxnExecutor<TState> detTxnExecutor;
        SortedDictionary<long, TaskCompletionSource> batchCommit;    // key: local bid

        // ACT execution
        IGlobalCoordGrain myGlobalCoord;
        Dictionary<long, Guid> coordinatorMap;
        NonDetTxnExecutor<TState> nonDetTxnExecutor;
        NonDetCommitter<TState> nonDetCommitter;
        Dictionary<string, ILocalCoordGrain> myLocalCoordPerSilo;
        // for hybrid serializability check
        CommitInfo commitInfo;
        
        // for grain migration
        bool underMigration;
        MyCounter numToBeRegisteredTxn;
        TaskCompletionSource allTxnGetContext;
        long highestCommittedLocalBidOnGrain;
        long highestCommittedGlobalBidOnGrain;
        Tuple<long, TaskCompletionSource> waitForLocalBidToCommit;
        Tuple<long, TaskCompletionSource> waitForGlobalBidToCommit;
        // in-memory cache for uncommitted states
        SortedDictionary<long, Tuple<DateTime, byte[]>> statePerBatch; // <localBid, timestamp, state>
        Dictionary<DateTime, byte[]> lastPreparedState;                // <timestamp, state>

        public TransactionExecutionGrain(
            ILoggingProtocol log, 
            IGrainPlacementCache grainPlacementCache, 
            IConnectionMultiplexer redis)
        {
            this.log = log;
            this.grainPlacementCache = grainPlacementCache;
            grainPlacement_db = redis.GetDatabase(Constants.Redis_GrainPlacementMap);
        }

        public Task CheckGC()
        {
            state.CheckGC();
            myScheduler.CheckGC();
            detTxnExecutor.CheckGC();
            nonDetTxnExecutor.CheckGC();
            nonDetCommitter.CheckGC();
            if (batchCommit.Count != 0) Console.WriteLine($"TransactionExecutionGrain: batchCommit.Count = {batchCommit.Count}");
            if (coordinatorMap.Count != 0) Console.WriteLine($"TransactionExecutionGrain: coordinatorMap.Count = {coordinatorMap.Count}");
            if (statePerBatch.Count > 1) Console.WriteLine($"TransactionExecutionGrain: statePerBatch.Count = {statePerBatch.Count}");
            return Task.CompletedTask;
        }

        /// <summary> Any type of message may cause the grain being activated </summary>
        public override async Task OnActivateAsync()
        {
            underMigration = true;

            myID = this.GetPrimaryKey();
            numToBeRegisteredTxn = new MyCounter();
            highestCommittedLocalBidOnGrain = -1;
            highestCommittedGlobalBidOnGrain = -1;
            waitForLocalBidToCommit = null;
            waitForGlobalBidToCommit = null;

            // transaction execution
            myScheduler = new TransactionScheduler(myID);
            batchCommit = new SortedDictionary<long, TaskCompletionSource>();
            coordinatorMap = new Dictionary<long, Guid>();
            commitInfo = new CommitInfo();
            commitInfo.highestCommittedLocalBidPerSilo.Add(RuntimeIdentity, -1);
            statePerBatch = new SortedDictionary<long, Tuple<DateTime, byte[]>>();
            lastPreparedState = new Dictionary<DateTime, byte[]> { { DateTime.MinValue, null } };

            // add its own info to local cache
            grainPlacementCache.AddUserGrain(myID, RuntimeIdentity);

            // select a random GrainPlacementManager locate in the current silo
            var managerID = grainPlacementCache.GetOneRandomPMInSilo(RuntimeIdentity);
            var myGrainPlacementManager = GrainFactory.GetGrain<IGrainPlacementManager>(managerID);

            // select a random GrainMigrationWorker locate in the current silo
            var workerID = grainPlacementCache.GetOneRandomMWInSilo(RuntimeIdentity);
            await grainPlacement_db.HashSetAsync(Constants.GrainIDPrefix + myID.ToString(), "MigrationWorker", workerID.ToString());

            state = new HybridState<TState>(new TState());

            // set up local and global coordinator info
            var localCoordID = grainPlacementCache.GetOneRandomCoordInSilo(RuntimeIdentity);
            var myLocalCoord = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
            hierarchicalCoord = grainPlacementCache.GetHierarchicalCoord();
            if (hierarchicalCoord)
            {
                var globalSiloAddress = grainPlacementCache.GetGlobalSiloAddress();
                var globalCoordID = grainPlacementCache.GetOneRandomCoordInSilo(globalSiloAddress);
                myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);
            }

            myLocalCoordPerSilo = new Dictionary<string, ILocalCoordGrain>();
            var res = grainPlacementCache.GetOneRandomLocalCoordPerSilo();
            foreach (var info in res)
            {
                var localCoord = GrainFactory.GetGrain<ILocalCoordGrain>(info.Value);
                myLocalCoordPerSilo.Add(info.Key, localCoord);
            }

            detTxnExecutor = new DetTxnExecutor<TState>(
                myID,
                RuntimeIdentity,
                GrainFactory,
                myScheduler,
                state,
                log,
                myGrainPlacementManager,
                commitInfo,
                statePerBatch);

            nonDetTxnExecutor = new NonDetTxnExecutor<TState>(
                myID,
                RuntimeIdentity,
                myLocalCoord,
                myGlobalCoord,
                hierarchicalCoord,
                myScheduler,
                state,
                commitInfo);

            nonDetCommitter = new NonDetCommitter<TState>(
                myID,
                coordinatorMap,
                state,
                log,
                grainPlacementCache,
                GrainFactory,
                commitInfo,
                lastPreparedState);
        }

        /// <summary> This interface is called to proactively to activate a transactional grain </summary>
        public async Task ActivateGrain(byte[] data = null)
        {
            if (data != null)
            {
                var myState = MessagePackSerializer.Deserialize<TState>(data);
                if (myState.PrintState() != myID.ToString()) throw new Exception($"ActivateGrain: myState {myState.PrintState()} is not myID {myID}");
                state.SetState(myState);

                // in case this grain is de-activated again before executing any PACT on this new silo
                if (log.IsLoggingEnabled()) await log.LocalBatchComplete(myID, -1, Guid.Empty, data, DateTime.Now);
            }
            
            // from now on, the grain can start processing transactions normally
            underMigration = false;
        }

        /// <summary> This interface is called to set a boundary for the grain to directly reject more StartTransaction requests </summary>
        public async Task StartMigration()
        {
            // set it true, so no more StartTransaction requests will be accepted
            underMigration = true;

            // wait for all accepted PACT StartTransaction requests get TxnContext
            if (numToBeRegisteredTxn.GetCount() == 0) return;
            allTxnGetContext = new TaskCompletionSource();
            await allTxnGetContext.Task;
            allTxnGetContext = null;
        }

        byte[] GetLastCommittedGrainStateFromCache(long lastCommittedLocalBid)
        {
            var t1 = lastPreparedState.First();
            var t2 = statePerBatch.ContainsKey(lastCommittedLocalBid) ? statePerBatch[lastCommittedLocalBid] : new Tuple<DateTime, byte[]>(DateTime.MinValue, null);

            if (t1.Key == DateTime.MinValue) return t2.Item2;      // did not find any state committed by ACTs 
            if (lastCommittedLocalBid == -1) return t1.Value;      // did not find any state committed by newly happened PACT batches
            return t2.Item1 > t1.Key ? t2.Item2 : t1.Value;
        }

        /// <summary> This interface is called to pro-actively de-activate the grain </summary>
        public async Task<Tuple<double, double, double, byte[]>> PrepareDeactivation(long localBid, long globalBid)
        {
            var start = DateTime.Now;

            // wait until the local bid and global bid have all committed
            var tasks = new List<Task>();
            if (highestCommittedLocalBidOnGrain < localBid)
            {
                waitForLocalBidToCommit = new Tuple<long, TaskCompletionSource>(localBid, new TaskCompletionSource());
                tasks.Add(waitForLocalBidToCommit.Item2.Task);
            }

            if (highestCommittedGlobalBidOnGrain < globalBid)
            {
                waitForGlobalBidToCommit = new Tuple<long, TaskCompletionSource>(globalBid, new TaskCompletionSource());
                tasks.Add(waitForGlobalBidToCommit.Item2.Task);
            }

            tasks.Add(myScheduler.WaitForLastNonDetNodeToComplete());
            await Task.WhenAll(tasks);
            waitForLocalBidToCommit = null;
            waitForGlobalBidToCommit = null;

            var waitTxnCommitTime = (DateTime.Now - start).TotalMilliseconds;

            start = DateTime.Now;
            //var res = await log.GetLastCommittedGrainStateFromLog(myID, highestCommittedLocalBidOnGrain);
            var res = GetLastCommittedGrainStateFromCache(highestCommittedLocalBidOnGrain);
            var getCommittedStateTime = (DateTime.Now - start).TotalMilliseconds;

            start = DateTime.Now;
            if (res == null) throw new Exception($"PrepareDeactivation: last committed state is null");
            var s = MessagePackSerializer.Deserialize<TState>(res);
            if (s.PrintState() != myID.ToString()) throw new Exception($"PrepareDeactivation: state {s.PrintState()} is not myID {myID}");

            var deserializeStateTime = (DateTime.Now - start).TotalMilliseconds;
            return new Tuple<double, double, double, byte[]>(waitTxnCommitTime, getCommittedStateTime, deserializeStateTime, res);
        }

        public async Task DeactivateGrain()
        {
            await Task.CompletedTask;
            DeactivateOnIdle();
        }

        /// <summary> This interface is called by clients to start a PACT </summary>
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<Guid> grainAccessInfo)
        {
            var receiveTxnTime = DateTime.Now;
            if (underMigration) throw new SnapperGrainMigrationException($"grain {myID}: Fail to StartTxn for PACT, grain is under migration, try again later");

            numToBeRegisteredTxn.Increment();
            var cxt = await detTxnExecutor.GetDetContext(grainAccessInfo);
            var getContextTime = DateTime.Now;

            if (underMigration && numToBeRegisteredTxn.Decrement()) allTxnGetContext.SetResult();

            // execute PACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var res = await ExecuteDet(call, cxt);
            var finishExeTime = DateTime.Now;
            var startExeTime = res.Item2;
            var resultObj = res.Item1;

            // wait for this batch to commit
            if (commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity] < cxt.localBid) await batchCommit[cxt.localBid].Task;
            var commitTime = DateTime.Now;
            var txnResult = new TransactionResult(resultObj);
            txnResult.registTime = (getContextTime - receiveTxnTime).TotalMilliseconds;
            txnResult.prepareTime = (startExeTime - getContextTime).TotalMilliseconds;
            txnResult.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            txnResult.commitTime = (commitTime - finishExeTime).TotalMilliseconds;
            return txnResult;
        }

        /// <summary> This interface is called by clients to start an ACT </summary>
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            var receiveTxnTime = DateTime.Now;
            if (underMigration) throw new SnapperGrainMigrationException($"grain {myID}: Fail to StartTxn for ACT, grain is under migration, try again later");
            var cxt = await nonDetTxnExecutor.GetNonDetContext();
            var getContextTime = DateTime.Now;
            
            // execute ACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var res1 = await ExecuteNonDet(call, cxt);
            var finishExeTime = DateTime.Now;
            var startExeTime = res1.Item2;
            var funcResult = res1.Item1;

            // check serializability and do 2PC
            var canCommit = !funcResult.exception;
            var res = new TransactionResult(funcResult.resultObj);
            var isPrepared = false;
            if (canCommit)
            {
                var result = nonDetCommitter.CheckSerializability(funcResult.globalScheduleInfo, funcResult.scheduleInfoPerSilo);

                canCommit = result.Item1;
                if (canCommit)
                {
                    isPrepared = true;
                    canCommit = await nonDetCommitter.CoordPrepare(cxt.globalTid, funcResult.grainOpInfo);
                }
                else
                {
                    if (result.Item2) res.Exp_Serializable = true;
                    else res.Exp_NotSureSerializable = true;
                }
            }
            else
            {
                // when deadlock = false && grainMigration = false, exception may from RW conflict
                res.Exp_Deadlock |= funcResult.Exp_Deadlock;  
                res.Exp_GrainMigration |= funcResult.Exp_GrainMigration;
            } 
            
            if (canCommit) await nonDetCommitter.CoordCommit(cxt.globalTid, funcResult);
            else
            {
                res.exception = true;
                await nonDetCommitter.CoordAbort(cxt.globalTid, funcResult.grainOpInfo, isPrepared);
            }

            // wait for maxBeforeLocalBid in each silo to commit
            if (canCommit)
            {
                var tasks = new Dictionary<string, Task<long>>();
                foreach (var siloInfo in funcResult.scheduleInfoPerSilo)
                {
                    var siloID = siloInfo.Key;
                    var bid = funcResult.scheduleInfoPerSilo[siloID].maxBeforeBid;
                    if (bid != -1) tasks.Add(siloID, myLocalCoordPerSilo[siloID].NewWaitBatchCommit(bid));
                }
                await Task.WhenAll(tasks.Values);

                foreach (var item in tasks) commitInfo.MergeCommitInfoOfSilo(item.Key, item.Value.Result);
            }

            var commitTime = DateTime.Now;
            res.registTime = (getContextTime - receiveTxnTime).TotalMilliseconds;
            res.prepareTime = (startExeTime - getContextTime).TotalMilliseconds;
            res.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            res.commitTime = (commitTime - finishExeTime).TotalMilliseconds;
            return res;
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public Task ReceiveBatchSchedule(LocalSubBatch batch)
        {
            // do garbage collection for committed local batches
            if (commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity] < batch.highestCommittedBid)
                commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity] = batch.highestCommittedBid;
            myScheduler.AckBatchCommit(commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity]);
            commitInfo.MergeCommitInfoOfSilo("", batch.highestCommittedGlobalBid);

            batchCommit.Add(batch.bid, new TaskCompletionSource());

            // register the local SubBatch info
            myScheduler.RegisterBatch(batch, batch.globalBid, commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity]);
            detTxnExecutor.BatchArrive(batch);

            return Task.CompletedTask;
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(long localBid, long globalBid)
        {
            // if this message comes after the grain is de-activated
            if (batchCommit.ContainsKey(localBid) == false) return Task.CompletedTask;

            if (commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity] < localBid)
            {
                commitInfo.highestCommittedLocalBidPerSilo[RuntimeIdentity] = localBid;
                myScheduler.AckBatchCommit(localBid);
            }
            commitInfo.highestCommittedGlobalBid = Math.Max(commitInfo.highestCommittedGlobalBid, globalBid);

            batchCommit[localBid].SetResult();
            batchCommit.Remove(localBid);

            // for grain migration
            if (highestCommittedLocalBidOnGrain < localBid)
            {
                var bid = statePerBatch.First().Key;
                while (bid < localBid)
                {
                    statePerBatch.Remove(bid);
                    bid = statePerBatch.First().Key;
                }

                highestCommittedLocalBidOnGrain = localBid;
                if (waitForLocalBidToCommit != null && waitForLocalBidToCommit.Item1 <= localBid && waitForLocalBidToCommit.Item2.Task.IsCompleted == false)
                    waitForLocalBidToCommit.Item2.SetResult();
            }

            if (highestCommittedGlobalBidOnGrain < globalBid)
            {
                highestCommittedGlobalBidOnGrain = globalBid;
                if (waitForGlobalBidToCommit != null && waitForGlobalBidToCommit.Item1 <= globalBid && waitForGlobalBidToCommit.Item2.Task.IsCompleted == false)
                    waitForGlobalBidToCommit.Item2.SetResult();
            }
            return Task.CompletedTask;
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public async Task<TState> GetState(TransactionContext cxt, AccessMode mode)
        {
            var isDet = cxt.localBid != -1;
            if (isDet) return detTxnExecutor.GetState(cxt.localTid, mode);
            else return await nonDetTxnExecutor.GetState(cxt.globalTid, mode);
        }

        public async Task<Tuple<object, DateTime>> ExecuteDet(FunctionCall call, TransactionContext cxt)
        {
            await detTxnExecutor.WaitForTurn(cxt);
            var time = DateTime.Now;
            var txnRes = await InvokeFunction(call, cxt);   // execute the function call;
            await detTxnExecutor.FinishExecuteDetTxn(cxt);
            detTxnExecutor.CleanUp(cxt.localTid);
            return new Tuple<object, DateTime>(txnRes.resultObj, time);
        }

        public async Task<Tuple<NonDetFuncResult, DateTime>> ExecuteNonDet(FunctionCall call, TransactionContext cxt)
        {
            if (underMigration)
            {
                var funcResult = new NonDetFuncResult();
                funcResult.Exp_GrainMigration = true;
                funcResult.exception = true;
                return new Tuple<NonDetFuncResult, DateTime>(funcResult, DateTime.Now);
            } 

            var canExecute = await nonDetTxnExecutor.WaitForTurn(cxt.globalTid);
            var time = DateTime.Now;
            if (canExecute == false)
            {
                var funcResult = new NonDetFuncResult();
                funcResult.Exp_Deadlock = true;
                funcResult.exception = true;
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                return new Tuple<NonDetFuncResult, DateTime>(funcResult, time);
            }
            else
            {
                var exception = false;
                Object resultObj = null;
                try
                {
                    var txnRes = await InvokeFunction(call, cxt);
                    resultObj = txnRes.resultObj;
                }
                catch (SnapperDeadlockException)
                {
                    // exceptions thrown from GetState will be caught here
                    exception = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Unexpected exception: {e.Message} {e.StackTrace}");
                    throw;
                }

                var funcResult = nonDetTxnExecutor.UpdateExecutionResult(cxt.globalTid);
                if (resultObj != null) funcResult.SetResultObj(resultObj);

                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                if (exception) CleanUp(cxt.globalTid);

                return new Tuple<NonDetFuncResult, DateTime>(funcResult, time);
            }
        }

        async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext cxt)
        {
            if (cxt.localBid == -1)
            {
                if (coordinatorMap.ContainsKey(cxt.globalTid)) throw new Exception($"InvokeFunction: globalTid {cxt.globalTid} is already in coordinatorMap");
                coordinatorMap.Add(cxt.globalTid, cxt.nonDetCoordID);
            }
            var mi = call.className.GetMethod(call.funcName);
            var t = (Task<TransactionResult>)mi.Invoke(this, new object[] { cxt, call.funcInput });
            return await t;
        }

        /// <summary> When execute a transaction, call this interface to make a cross-grain function invocation </summary>
        public Task<TransactionResult> CallGrain(TransactionContext cxt, Guid grainID, FunctionCall call)
        {
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID, Constants.grainClassName);
            var isDet = cxt.localBid != -1;
            if (isDet) return detTxnExecutor.CallGrain(cxt, call, grain);
            else return nonDetTxnExecutor.CallGrain(cxt, call, grain);
        }

        public async Task<bool> Prepare(long tid, bool isReader)
        {
            var vote = await nonDetCommitter.Prepare(tid, isReader);
            if (isReader) CleanUp(tid);
            return vote;
        }

        // only writer grain needs 2nd phase of 2PC
        public async Task Commit(long tid, long maxBeforeLocalBid, long maxBeforeGlobalBid)   
        {
            nonDetTxnExecutor.Commit(maxBeforeLocalBid, maxBeforeGlobalBid);
            await nonDetCommitter.Commit(tid);
            CleanUp(tid);
        }

        public Task Abort(long tid)
        {
            nonDetCommitter.Abort(tid);
            CleanUp(tid);
            return Task.CompletedTask;
        }

        void CleanUp(long tid)
        {
            if (coordinatorMap.ContainsKey(tid)) coordinatorMap.Remove(tid);
            myScheduler.scheduleInfo.CompleteNonDetTxn(tid);
        }
    }
}