using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;
using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans;
using Orleans.Runtime;

namespace SnapperExperimentWorker
{
    internal class ConsumerThread
    {
        int dist_numEmit = 0;
        int dist_numCommit = 0;
        List<double> dist_latency = new List<double>();
        List<double> dist_registTxnTime = new List<double>();      // grain receive txn  ==>  get basic context info
        List<double> dist_prepareTxnTime = new List<double>();     // get context info   ==>  start execute txn
        List<double> dist_executeTxnTime = new List<double>();     // start execute txn  ==>  finish execute txn
        List<double> dist_commitTxnTime = new List<double>();      // finish execute txn ==>  batch committed

        int non_dist_numEmit = 0;
        int non_dist_numCommit = 0;
        List<double> non_dist_latency = new List<double>();
        List<double> non_dist_registTxnTime = new List<double>();      // grain receive txn  ==>  get basic context info
        List<double> non_dist_prepareTxnTime = new List<double>();     // get context info   ==>  start execute txn
        List<double> non_dist_executeTxnTime = new List<double>();     // start execute txn  ==>  finish execute txn
        List<double> non_dist_commitTxnTime = new List<double>();      // finish execute txn ==>  batch committed

        // only for ACT
        int dist_numDeadlock = 0;
        int dist_numNotSerializable = 0;
        int dist_numNotSureSerializable = 0;
        int dist_numGrainMigration = 0;

        int non_dist_numDeadlock = 0;
        int non_dist_numNotSerializable = 0;
        int non_dist_numNotSureSerializable = 0;
        int non_dist_numGrainMigration = 0;

        // only for grain migration
        int dist_numMigrationExp = 0;
        int non_dist_numMigrationExp = 0;
        List<Tuple<DateTime, DateTime>> dist_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
        List<Tuple<DateTime, DateTime>> non_dist_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
        List<Tuple<DateTime, DateTime>> dist_abort_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
        List<Tuple<DateTime, DateTime>> non_dist_abort_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();

        List<Task<TransactionResult>> tasks = new List<Task<TransactionResult>>();
        Dictionary<Task<TransactionResult>, Tuple<DateTime, bool>> reqs = new Dictionary<Task<TransactionResult>, Tuple<DateTime, bool>>();

        readonly bool isDet;
        readonly int pipeSize;
        readonly WorkloadConfiguration workload;
        readonly IBenchmark benchmark;
        readonly IClusterClient client;
        ConcurrentQueue<RequestData> queue;
        readonly Stopwatch globalWatch;
        bool[] isEpochFinish;
        readonly bool[] isProducerFinish;

        readonly bool isGrainMigrationExperiment;
        readonly bool[] isMigrationDone;
        readonly WorkloadGenerator workloadGenerator;

        public ConsumerThread(
            bool isDet,
            WorkloadConfiguration workload,
            IBenchmark benchmark,
            IClusterClient client,
            ConcurrentQueue<RequestData> queue,
            Stopwatch globalWatch,
            bool[] isEpochFinish,
            bool[] isProducerFinish,
            bool isGrainMigrationExperiment,
            bool[] isMigrationDone,
            WorkloadGenerator workloadGenerator)
        {
            this.isDet = isDet;
            pipeSize = isDet ? workload.pactPipeSize : workload.actPipeSize;
            this.workload = workload;
            this.benchmark = benchmark;
            this.client = client;
            this.queue = queue;
            this.globalWatch = globalWatch;
            this.isEpochFinish = isEpochFinish;
            this.isProducerFinish = isProducerFinish;
            this.isGrainMigrationExperiment = isGrainMigrationExperiment;
            this.isMigrationDone = isMigrationDone;
            this.workloadGenerator = workloadGenerator;
        }

        public async Task<WorkloadResult> RunEpoch(int eIndex)
        {
            var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            if (isGrainMigrationExperiment) await RunEpochForGrainMigrationExp(eIndex);
            else await RunEpochForRegularExp(eIndex);
            isEpochFinish[eIndex] = true;   // which means producer doesn't need to produce more requests

            // Wait for the tasks exceeding epoch time and also count them into results
            while (tasks.Count != 0) await WaitForTxnTaskAsync();
            long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            globalWatch.Stop();

            if (isDet)
                Console.WriteLine($"PACT: dist_tp = {1000 * dist_numCommit / (endTime - startTime)}, " +
                                    $"non_dist_tp = {1000 * non_dist_numCommit / (endTime - startTime)}");
            else
            {
                if (workload.pactPercent > 0)    // hybrid
                {
                    Console.WriteLine($"ACT: dist_num_commit = {dist_numCommit}, dist_tp = {1000 * dist_numCommit / (endTime - startTime)}, " +
                                        $"dist_abort = {dist_numEmit - dist_numCommit}, dist_numDeadlock = {dist_numDeadlock}, dist_numNotSerializable = {dist_numNotSerializable}, dist_numNotSureSerializable = {dist_numNotSureSerializable}, dist_numGrainMigration = {dist_numGrainMigration}, " +
                                        $"non_dist_num_commit = {non_dist_numCommit}, non_dist_tp = {1000 * non_dist_numCommit / (endTime - startTime)}, " +
                                        $"non_dist_abort = {non_dist_numEmit - non_dist_numCommit}, non_dist_numDeadlock = {non_dist_numDeadlock}, non_dist_numNotSerializable = {non_dist_numNotSerializable}, non_dist_numNotSureSerializable = {non_dist_numNotSureSerializable}, non_dist_numGrainMigration = {non_dist_numGrainMigration}");
                }
                else
                {
                    Console.WriteLine($"ACT: dist_tp = {1000 * dist_numCommit / (endTime - startTime)}, dist_abort% = {Helper.ChangeFormat((dist_numEmit - dist_numCommit) * 100.0 / dist_numEmit, 2)}%, " +
                                           $"non_dist_tp = {1000 * non_dist_numCommit / (endTime - startTime)}, non_dist_abort% = {Helper.ChangeFormat((non_dist_numEmit - non_dist_numCommit) * 100.0 / non_dist_numEmit, 2)}%");
                }
            }

            Console.WriteLine($"isDet consumer {isDet}: num_dist_migration_exp = {dist_numMigrationExp}, num_non_dist_migration_exp = {non_dist_numMigrationExp}");
            
            var res = new WorkloadResult();
            res.SetTime(startTime, endTime);
            res.SetNumber(isDet, true, dist_numCommit, dist_numEmit, dist_numDeadlock, dist_numNotSerializable, dist_numNotSureSerializable, dist_numGrainMigration);
            res.SetNumber(isDet, false, non_dist_numCommit, non_dist_numEmit, non_dist_numDeadlock, non_dist_numNotSerializable, non_dist_numNotSureSerializable, non_dist_numGrainMigration);
            res.SetLatency(isDet, true, dist_latency, dist_registTxnTime, dist_prepareTxnTime, dist_executeTxnTime, dist_commitTxnTime);
            res.SetLatency(isDet, false, non_dist_latency, non_dist_registTxnTime, non_dist_prepareTxnTime, non_dist_executeTxnTime, non_dist_commitTxnTime);
            if (isGrainMigrationExperiment) res.SetTxnStartAndEndTime(dist_txn_startAndEndTime, non_dist_txn_startAndEndTime, dist_abort_txn_startAndEndTime, non_dist_abort_txn_startAndEndTime);
            return res;
        }

        async Task RunEpochForRegularExp(int eIndex)
        {
            RequestData txn;
            do
            {
                while (tasks.Count < pipeSize && queue.TryDequeue(out txn))
                {
                    var startTxnTime = DateTime.Now;
                    var newTask = benchmark.NewTransaction(client, txn);

                    if (txn.isDistTxn) dist_numEmit++;
                    else non_dist_numEmit++;

                    reqs.Add(newTask, new Tuple<DateTime, bool>(startTxnTime, txn.isDistTxn));
                    tasks.Add(newTask);
                }
                if (tasks.Count != 0) await WaitForTxnTaskAsync();
            }
            while (globalWatch.ElapsedMilliseconds < Constants.epochDurationMSecs && (queue.Count != 0 || !isProducerFinish[eIndex]));
        }

        async Task RunEpochForGrainMigrationExp(int eIndex)
        {
            var startTime = DateTime.Now;
            var startTimeReset = false;
            do
            {
                if (isMigrationDone[eIndex] && startTimeReset == false)
                {
                    startTimeReset = true;
                    startTime = DateTime.Now;
                    Console.WriteLine($"ConsumerThread: grain migration is done, run for more time...");
                } 

                while (tasks.Count < pipeSize)
                {
                    var startTxnTime = DateTime.Now;
                    var txn = workloadGenerator.GenerateSmallBankTransactionForGrainMigrationExperiment();
                    var newTask = benchmark.NewTransaction(client, txn);

                    if (txn.isDistTxn) dist_numEmit++;
                    else non_dist_numEmit++;

                    reqs.Add(newTask, new Tuple<DateTime, bool>(startTxnTime, txn.isDistTxn));
                    tasks.Add(newTask);
                }
                if (tasks.Count != 0) await WaitForTxnTaskAsync();
            }
            while (isMigrationDone[eIndex] == false || startTimeReset == false || (DateTime.Now - startTime).TotalMilliseconds <= Constants.beforeAndAfterDurationMSecs);
            Console.WriteLine($"ConsumerThread: isMigrationDone = {isMigrationDone[eIndex]}, time = {(DateTime.Now - startTime).TotalMilliseconds}ms");
        }

        async Task WaitForTxnTaskAsync()
        {
            var task = await Task.WhenAny(tasks);
            var endTxnTime = DateTime.Now;
            var isDistTxn = reqs[task].Item2;
            var noException = true;
            var migrationExp = false;
            try
            {
                // Needed to catch exception of individual task (not caught by Snapper's exception) which would not be thrown by WhenAny
                await task;
            }
            catch (SnapperGrainMigrationException _)    
            {
                // this exception only happens when Snapper start migrating the grain and the grain receives the StartTxn requests
                migrationExp = true;
                noException = false;
            }
            catch (OrleansException _)   
            {
                // this exception can happen when the grain is de-activated and the StarTxn request cannot be delivered
                // it can also happen when an OrleansTxn aborts
                migrationExp = true;
                noException = false;
            }
            catch (Exception e)         // this exception is only related to OrleansTransaction
            {
                Console.WriteLine($"Unexpected exception: {e.Message} {e.StackTrace}");
                Debug.Assert(false);    //this should not happen
            }

            if (noException)
            {
                if (task.Result.exception == false)
                {
                    if (isDistTxn)
                    {
                        dist_numCommit++;
                        dist_latency.Add((endTxnTime - reqs[task].Item1).TotalMilliseconds);
                        dist_registTxnTime.Add(task.Result.registTime);
                        dist_prepareTxnTime.Add(task.Result.prepareTime);
                        dist_executeTxnTime.Add(task.Result.executeTime);
                        dist_commitTxnTime.Add(task.Result.commitTime);

                        if (isGrainMigrationExperiment)
                            dist_txn_startAndEndTime.Add(new Tuple<DateTime, DateTime>(reqs[task].Item1, endTxnTime));
                    }
                    else
                    {
                        non_dist_numCommit++;
                        non_dist_latency.Add((endTxnTime - reqs[task].Item1).TotalMilliseconds);
                        non_dist_registTxnTime.Add(task.Result.registTime);
                        non_dist_prepareTxnTime.Add(task.Result.prepareTime);
                        non_dist_executeTxnTime.Add(task.Result.executeTime);
                        non_dist_commitTxnTime.Add(task.Result.commitTime);

                        if (isGrainMigrationExperiment)
                            non_dist_txn_startAndEndTime.Add(new Tuple<DateTime, DateTime>(reqs[task].Item1, endTxnTime));
                    }
                }
                else
                {
                    if (isDistTxn)
                    {
                        if (task.Result.Exp_Serializable) dist_numNotSerializable++;
                        else if (task.Result.Exp_NotSureSerializable) dist_numNotSureSerializable++;
                        else if (task.Result.Exp_Deadlock) dist_numDeadlock++;
                        else if (task.Result.Exp_GrainMigration) dist_numGrainMigration++;

                        if (isGrainMigrationExperiment)
                            dist_abort_txn_startAndEndTime.Add(new Tuple<DateTime, DateTime>(reqs[task].Item1, endTxnTime));
                    }
                    else
                    {
                        if (task.Result.Exp_Serializable) non_dist_numNotSerializable++;
                        else if (task.Result.Exp_NotSureSerializable) non_dist_numNotSureSerializable++;
                        else if (task.Result.Exp_Deadlock) non_dist_numDeadlock++;
                        else if (task.Result.Exp_GrainMigration) non_dist_numGrainMigration++;

                        if (isGrainMigrationExperiment)
                            non_dist_abort_txn_startAndEndTime.Add(new Tuple<DateTime, DateTime>(reqs[task].Item1, endTxnTime));
                    }
                }
            }
            else if (migrationExp == true)
            {
                if (isDistTxn) dist_abort_txn_startAndEndTime.Add(new Tuple<DateTime, DateTime>(reqs[task].Item1, endTxnTime));
                else non_dist_abort_txn_startAndEndTime.Add(new Tuple<DateTime, DateTime>(reqs[task].Item1, endTxnTime));
            }
            tasks.Remove(task);
            reqs.Remove(task);
        }
    }
}