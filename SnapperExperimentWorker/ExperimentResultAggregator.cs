using MathNet.Numerics.Statistics;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Utilities;

namespace SnapperExperimentWorker
{
    public class ExperimentResultAggregator
    {
        readonly string experimentID;
        readonly int numLocalSilo;
        readonly ImplementationType implementationType;
        readonly bool isLoggingEnabled;
        readonly int batchSizeInMSecsBasic;

        readonly bool isGrainMigrationExp;
        readonly int numEpoch;
        readonly WorkloadConfiguration workload;
        WorkloadResult[,] results;
        readonly int[] percentilesToCalculate;

        public ExperimentResultAggregator(string experimentID, int numLocalSilo, ImplementationType implementationType, bool isLoggingEnabled, int batchSizeInMSecsBasic,
            bool isGrainMigrationExp, int numEpoch, WorkloadConfiguration workload)
        {
            this.experimentID = experimentID;
            this.numLocalSilo = numLocalSilo;
            this.implementationType = implementationType;
            this.isLoggingEnabled = isLoggingEnabled;
            this.batchSizeInMSecsBasic = batchSizeInMSecsBasic;

            this.workload = workload;
            this.numEpoch = numEpoch;
            this.isGrainMigrationExp = isGrainMigrationExp;
            results = new WorkloadResult[numEpoch, numLocalSilo];
            percentilesToCalculate = new int[] { 50, 90, 99 };
        }

        public void SetResult(int i, int j, WorkloadResult result)
        {
            results[i, j] = result;
        }

        public static WorkloadResult AggregateResultForEpoch(WorkloadResult[] result)
        {
            var aggResult = result[0];
            for (int i = 1; i < result.Length; i++) aggResult.MergeData(result[i]);
            return aggResult;
        }

        public bool AggregateResultsAndPrint(int numRun)
        {
            Console.WriteLine("Aggregating results and printing");

            var pact_dist_tp = new List<double>();
            var pact_non_dist_tp = new List<double>();

            var act_total_abort = new List<double>();

            var act_dist_tp = new List<double>();
            var act_dist_abort = new List<double>();
            var act_dist_abort_deadlock = new List<double>();
            var act_dist_abort_notSerializable = new List<double>();
            var act_dist_abort_notSureSerializable = new List<double>();
            var act_dist_abort_grainMigration = new List<double>();

            var act_non_dist_tp = new List<double>();
            var act_non_dist_abort = new List<double>();
            var act_non_dist_abort_deadlock = new List<double>();
            var act_non_dist_abort_notSerializable = new List<double>();
            var act_non_dist_abort_notSureSerializable = new List<double>();
            var act_non_dist_abort_grainMigration = new List<double>();

            // latencies
            var pact_dist_latencies = new BasicLatencyInfo();
            var pact_non_dist_latencies = new BasicLatencyInfo();

            var act_dist_latencies = new BasicLatencyInfo();
            var act_non_dist_latencies = new BasicLatencyInfo();

            // only for grain migration experiment
            var dist_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
            var non_dist_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
            var dist_abort_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
            var non_dist_abort_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();

            var numWarmupEpoch = isGrainMigrationExp ? 0 : Constants.numWarmupEpoch;
            for (int e = numWarmupEpoch; e < numEpoch; e++) 
            {
                var result = new WorkloadResult[numLocalSilo];
                for (int i = 0; i < numLocalSilo; i++) result[i] = results[e, i];
                var aggResultForOneEpoch = AggregateResultForEpoch(result);

                pact_dist_latencies.MergeData(aggResultForOneEpoch.pact_dist_result.latencies);
                pact_non_dist_latencies.MergeData(aggResultForOneEpoch.pact_non_dist_result.latencies);
                act_dist_latencies.MergeData(aggResultForOneEpoch.act_dist_result.basic_result.latencies);
                act_non_dist_latencies.MergeData(aggResultForOneEpoch.act_non_dist_result.basic_result.latencies);

                dist_txn_startAndEndTime.AddRange(aggResultForOneEpoch.dist_txn_startAndEndTime);
                non_dist_txn_startAndEndTime.AddRange(aggResultForOneEpoch.non_dist_txn_startAndEndTime);
                dist_abort_txn_startAndEndTime.AddRange(aggResultForOneEpoch.dist_abort_txn_startAndEndTime);
                non_dist_abort_txn_startAndEndTime.AddRange(aggResultForOneEpoch.non_dist_abort_txn_startAndEndTime);

                var data = aggResultForOneEpoch.GetPrintData();
                pact_dist_tp.Add(data.pact_dist_print.throughput);
                pact_non_dist_tp.Add(data.pact_non_dist_print.throughput);

                act_total_abort.Add(data.act_total_abort);

                act_dist_tp.Add(data.act_dist_print.basic_data.throughput);
                act_dist_abort.Add(data.act_dist_print.abort);
                act_dist_abort_deadlock.Add(data.act_dist_print.abort_deadlock);
                act_dist_abort_notSerializable.Add(data.act_dist_print.abort_notSerializable);
                act_dist_abort_notSureSerializable.Add(data.act_dist_print.abort_notSureSerializable);
                act_dist_abort_grainMigration.Add(data.act_dist_print.abort_grainMigration);

                act_non_dist_tp.Add(data.act_non_dist_print.basic_data.throughput);
                act_non_dist_abort.Add(data.act_non_dist_print.abort);
                act_non_dist_abort_deadlock.Add(data.act_non_dist_print.abort_deadlock);
                act_non_dist_abort_notSerializable.Add(data.act_non_dist_print.abort_notSerializable);
                act_non_dist_abort_notSureSerializable.Add(data.act_non_dist_print.abort_notSureSerializable);
                act_non_dist_abort_grainMigration.Add(data.act_non_dist_print.abort_grainMigration);
            }
            
            if (experimentID[0] != Constants.grainMigrationExpID && implementationType != ImplementationType.ORLEANSTXN && numRun < Constants.maxNumReRun - 1)
            {
                if (CheckSdSafeRange(pact_dist_tp) == false) return false;
                if (CheckSdSafeRange(pact_non_dist_tp) == false) return false;
                if (CheckSdSafeRange(act_dist_tp) == false) return false;
                if (CheckSdSafeRange(act_non_dist_tp) == false) return false;
            }
            
            using (var file = new StreamWriter(Constants.resultPath, true))
            {
                // experiment setting
                file.Write($"Fig.{experimentID} ");
                file.Write($"{numLocalSilo} {implementationType} {isLoggingEnabled} {batchSizeInMSecsBasic} ");
                file.Write($"{workload.txnSize} {workload.txnDistLevel} {workload.pactPercent} {workload.distPercent} {workload.grainSkewness} {workload.actPipeSize} {workload.pactPipeSize} {workload.migrationPipeSize} ");

                // throughput
                var pact_dist = pact_dist_tp.Mean();
                var pact_non_dist = pact_non_dist_tp.Mean();
                var act_dist = act_dist_tp.Mean();
                var act_non_dist = act_non_dist_tp.Mean();
                file.Write($"{Helper.ChangeFormat(pact_dist, 0)} {Helper.ChangeFormat(pact_non_dist, 0)} {Helper.ChangeFormat(pact_dist + pact_non_dist, 0)} " +
                           $"{Helper.ChangeFormat(act_dist, 0)} {Helper.ChangeFormat(act_non_dist, 0)} {Helper.ChangeFormat(act_dist + act_non_dist, 0)} ");

                // ACT total abort rate
                file.Write($"{Helper.ChangeFormat(act_total_abort.Mean(), 2)}% ");

                // abort reasons
                var act_dist_abort_rw = 100.0 - act_dist_abort_deadlock.Mean() - act_dist_abort_notSerializable.Mean() - act_dist_abort_notSureSerializable.Mean() - act_dist_abort_grainMigration.Mean();
                file.Write($"{Helper.ChangeFormat(act_dist_abort.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_dist_abort_rw, 2)}% " +
                           $"{Helper.ChangeFormat(act_dist_abort_deadlock.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_dist_abort_notSerializable.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_dist_abort_notSureSerializable.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_dist_abort_grainMigration.Mean(), 2)}% ");

                var act_non_dist_abort_rw = 100.0 - act_non_dist_abort_deadlock.Mean() - act_non_dist_abort_notSerializable.Mean() - act_non_dist_abort_notSureSerializable.Mean() - act_non_dist_abort_grainMigration.Mean();
                file.Write($"{Helper.ChangeFormat(act_non_dist_abort.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_non_dist_abort_rw, 2)}% " +
                           $"{Helper.ChangeFormat(act_non_dist_abort_deadlock.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_non_dist_abort_notSerializable.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_non_dist_abort_notSureSerializable.Mean(), 2)}% " +
                           $"{Helper.ChangeFormat(act_non_dist_abort_grainMigration.Mean(), 2)}% ");

                // PACT-dist: percentile latencies + breakdone latencies
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(pact_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                }
                file.Write($"{Helper.ChangeFormat(pact_dist_latencies.registTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(pact_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(pact_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(pact_dist_latencies.commitTxnTime.Mean(), 1)} ");

                // PACT-non-dist: percentile latencies + breakdone latencies
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(pact_non_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                }
                file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.registTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.commitTxnTime.Mean(), 1)} ");

                // ACT-dist: percentile latencies + breakdone latencies
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(act_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                }
                file.Write($"{Helper.ChangeFormat(act_dist_latencies.registTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(act_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(act_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(act_dist_latencies.commitTxnTime.Mean(), 1)} ");

                // ACT-non-dist: percentile latencies + breakdone latencies
                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(act_non_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                }
                file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.registTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.commitTxnTime.Mean(), 1)} ");

                file.WriteLine();
            }

            if (isGrainMigrationExp)
            {
                var minWindow = long.MaxValue;
                var maxWindow = long.MinValue;

                var numDistTxnPerWindow = new SortedDictionary<long, MyCounter>();
                var numNonDistTxnPerWindow = new SortedDictionary<long, MyCounter>();
                var numDistAbortTxnPerWindow = new SortedDictionary<long, MyCounter>();
                var numNonDistAbortTxnPerWindow = new SortedDictionary<long, MyCounter>();

                var distTxnLatenciesPerWindow = new SortedDictionary<long, List<double>>();
                var nonDistTxnLatenciesPerWindow = new SortedDictionary<long, List<double>>();

                foreach (var time in dist_txn_startAndEndTime)
                {
                    var windowID = GetWindowID(time.Item2);
                    minWindow = Math.Min(minWindow, windowID);
                    maxWindow = Math.Max(maxWindow, windowID);
                    if (numDistTxnPerWindow.ContainsKey(windowID) == false) numDistTxnPerWindow.Add(windowID, new MyCounter());
                    if (distTxnLatenciesPerWindow.ContainsKey(windowID) == false) distTxnLatenciesPerWindow.Add(windowID, new List<double>());
                    
                    numDistTxnPerWindow[windowID].Increment();
                    distTxnLatenciesPerWindow[windowID].Add((time.Item2 - time.Item1).TotalMilliseconds);
                }

                foreach (var time in non_dist_txn_startAndEndTime)
                {
                    var windowID = GetWindowID(time.Item2);
                    minWindow = Math.Min(minWindow, windowID);
                    maxWindow = Math.Max(maxWindow, windowID);
                    if (numNonDistTxnPerWindow.ContainsKey(windowID) == false) numNonDistTxnPerWindow.Add(windowID, new MyCounter());
                    if (nonDistTxnLatenciesPerWindow.ContainsKey(windowID) == false) nonDistTxnLatenciesPerWindow.Add(windowID, new List<double>());
                    
                    numNonDistTxnPerWindow[windowID].Increment();
                    nonDistTxnLatenciesPerWindow[windowID].Add((time.Item2 - time.Item1).TotalMilliseconds);
                }

                foreach (var time in dist_abort_txn_startAndEndTime)
                {
                    var windowID = GetWindowID(time.Item2);
                    minWindow = Math.Min(minWindow, windowID);
                    maxWindow = Math.Max(maxWindow, windowID);
                    if (numDistAbortTxnPerWindow.ContainsKey(windowID) == false) numDistAbortTxnPerWindow.Add(windowID, new MyCounter());
                    numDistAbortTxnPerWindow[windowID].Increment();
                }

                foreach (var time in non_dist_abort_txn_startAndEndTime)
                {
                    var windowID = GetWindowID(time.Item2);
                    minWindow = Math.Min(minWindow, windowID);
                    maxWindow = Math.Max(maxWindow, windowID);
                    if (numNonDistAbortTxnPerWindow.ContainsKey(windowID) == false) numNonDistAbortTxnPerWindow.Add(windowID, new MyCounter());
                    numNonDistAbortTxnPerWindow[windowID].Increment();
                }

                var fileName = Constants.dataPath + $"tp-GrainMigration-{workload.pactPercent}%PACT.txt";
                using (var file = new StreamWriter(fileName, true))
                {
                    for (long w = minWindow; w <= maxWindow; w++)
                    {
                        var numDistTxn = numDistTxnPerWindow.ContainsKey(w) ? numDistTxnPerWindow[w].GetCount() : 0;
                        var numNonDistTxn = numNonDistTxnPerWindow.ContainsKey(w) ? numNonDistTxnPerWindow[w].GetCount() : 0;
                        var numDistAbortTxn = numDistAbortTxnPerWindow.ContainsKey(w) ? numDistAbortTxnPerWindow[w].GetCount() : 0;
                        var numNonDistAbortTxn = numNonDistAbortTxnPerWindow.ContainsKey(w) ? numNonDistAbortTxnPerWindow[w].GetCount() : 0;
                        var distTxnLatency = distTxnLatenciesPerWindow.ContainsKey(w) ? (distTxnLatenciesPerWindow[w].Count == 0 ? 0 : distTxnLatenciesPerWindow[w].Average()) : 0;
                        var nonDistTxnLatency = nonDistTxnLatenciesPerWindow.ContainsKey(w) ? (nonDistTxnLatenciesPerWindow[w].Count == 0 ? 0 : nonDistTxnLatenciesPerWindow[w].Average()) : 0;

                        var total_tp = (numDistTxn + numNonDistTxn) * 1000 / Constants.MeasurementDurationMSecs;
                        var total_abort = (numDistAbortTxn + numNonDistAbortTxn) * 100.0 / (numDistAbortTxn + numNonDistAbortTxn + numDistTxn + numNonDistTxn);
                        var dist_tp = numDistTxn * 1000 / Constants.MeasurementDurationMSecs;
                        var non_dist_tp = numNonDistTxn * 1000 / Constants.MeasurementDurationMSecs;
                        var dist_abort = numDistAbortTxn * 100.0 / (numDistAbortTxn + numDistTxn);
                        var non_dist_abort = numNonDistAbortTxn * 100.0 / (numNonDistAbortTxn + numNonDistTxn);

                        var str = $"{w - minWindow} {total_tp} {Helper.ChangeFormat(total_abort, 2)}% {dist_tp} {non_dist_tp} " +
                            $"{Helper.ChangeFormat(dist_abort, 2)}% {Helper.ChangeFormat(non_dist_abort, 2)}% " +
                            $"{Helper.ChangeFormat(distTxnLatency, 1)} {Helper.ChangeFormat(nonDistTxnLatency, 1)}";
                        file.WriteLine(str);
                    }
                }
            }
               
            return true;
        }

        long GetWindowID(DateTime txnEndTime) => (long)((txnEndTime - DateTime.MinValue).TotalMilliseconds / Constants.MeasurementDurationMSecs);

        bool CheckSdSafeRange(List<double> data)
        { 
            if (data.Count != 0) return data.StandardDeviation() <= data.Mean() * Constants.sdSafeRange;
            return true;
        } 
    }
}