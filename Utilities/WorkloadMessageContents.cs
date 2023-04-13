using System;
using System.Collections.Generic;
using MessagePack;

namespace Utilities
{
    [MessagePackObject]
    public class WorkloadConfiguration
    {
        [Key(0)]
        public int txnSize;

        [Key(1)]
        public int txnDistLevel;

        [Key(2)]
        public int batchSizeInMSecsBasic;

        [Key(3)]
        public int pactPercent;

        [Key(4)]
        public int distPercent;

        [Key(5)]
        public double grainSkewness;
        [Key(6)]
        public int actPipeSize;
        [Key(7)]
        public int pactPipeSize;
        [Key(8)]
        public int migrationPipeSize;
        
        public WorkloadConfiguration(
            int txnSize,
            int txnDistLevel,
            int batchSizeInMSecsBasic,
            int pactPercent,
            int distPercent,
            double grainSkewness, 
            int actPipeSize, 
            int pactPipeSize,
            int migrationPipeSize)
        { 
            this.txnSize = txnSize;
            this.txnDistLevel = txnDistLevel;
            this.batchSizeInMSecsBasic = batchSizeInMSecsBasic;
            this.pactPercent = pactPercent;
            this.distPercent = distPercent;
            this.grainSkewness = grainSkewness;
            this.actPipeSize = actPipeSize;
            this.pactPipeSize = pactPipeSize;
            this.migrationPipeSize = migrationPipeSize;
        }
    }

    [MessagePackObject]
    public class BasicLatencyInfo
    {
        [Key(0)]
        public List<double> latency;
        [Key(1)]
        public List<double> registTxnTime;
        [Key(2)]
        public List<double> prepareTxnTime;
        [Key(3)]
        public List<double> executeTxnTime;
        [Key(4)]
        public List<double> commitTxnTime;

        public BasicLatencyInfo()
        {
            latency = new List<double>();
            registTxnTime = new List<double>();
            prepareTxnTime = new List<double>();
            executeTxnTime = new List<double>();
            commitTxnTime = new List<double>();
        }

        public void SetLatency(List<double> latency, List<double> registTxnTime, List<double> prepareTxnTime, List<double> executeTxnTime, List<double> commitTxnTime)
        {
            this.latency = latency;
            this.registTxnTime = registTxnTime;
            this.prepareTxnTime = prepareTxnTime;
            this.executeTxnTime = executeTxnTime;
            this.commitTxnTime = commitTxnTime;
        }

        public void MergeData(BasicLatencyInfo latencies)
        {
            latency.AddRange(latencies.latency);
            registTxnTime.AddRange(latencies.registTxnTime);
            prepareTxnTime.AddRange(latencies.prepareTxnTime);
            executeTxnTime.AddRange(latencies.executeTxnTime);
            commitTxnTime.AddRange(latencies.commitTxnTime);
        }
    }

    [MessagePackObject]
    public class BasicResult
    {
        [Key(0)]
        public int numCommit;
        [Key(1)]
        public BasicLatencyInfo latencies;

        public BasicResult()
        {
            latencies = new BasicLatencyInfo();
        }

        public BasicResult(int numCommit)
        {
            this.numCommit = numCommit;
            latencies = new BasicLatencyInfo();
        }

        public void MergeData(BasicResult res)
        {
            numCommit += res.numCommit;
            latencies.MergeData(res.latencies);
        }
    }

    [MessagePackObject]
    public class BasicACTResult
    {
        [Key(0)]
        public BasicResult basic_result;
        [Key(1)]
        public int numEmit;
        [Key(2)]
        public int numDeadlock;
        [Key(3)]
        public int numNotSerializable;
        [Key(4)]
        public int numNotSureSerializable;
        [Key(5)]
        public int numGrainMigration;

        public BasicACTResult()
        {
            basic_result = new BasicResult();
        }

        public BasicACTResult(int numCommit, int numEmit, int numDeadlock, int numNotSerializable, int numNotSureSerializable, int numGrainMigration)
        {
            basic_result = new BasicResult(numCommit);
            this.numEmit = numEmit;
            this.numDeadlock = numDeadlock;
            this.numNotSerializable = numNotSerializable;
            this.numNotSureSerializable = numNotSureSerializable;
            this.numGrainMigration = numGrainMigration;
        }

        public void MergeData(BasicACTResult res)
        {
            basic_result.MergeData(res.basic_result);
            numEmit += res.numEmit;
            numDeadlock += res.numDeadlock;
            numNotSerializable += res.numNotSerializable;
            numNotSureSerializable += res.numNotSureSerializable;
            numGrainMigration += res.numGrainMigration;
        }
    }

    [MessagePackObject]
    public class WorkloadResult
    {
        [Key(0)]
        public long startTime;
        [Key(1)]
        public long endTime;

        [Key(2)]
        public BasicResult pact_dist_result;
        [Key(3)]
        public BasicResult pact_non_dist_result;

        [Key(4)]
        public BasicACTResult act_dist_result;
        [Key(5)]
        public BasicACTResult act_non_dist_result;

        [Key(6)]
        public List<Tuple<DateTime, DateTime>> dist_txn_startAndEndTime;
        [Key(7)]
        public List<Tuple<DateTime, DateTime>> non_dist_txn_startAndEndTime;
        [Key(8)]
        public List<Tuple<DateTime, DateTime>> dist_abort_txn_startAndEndTime;
        [Key(9)]
        public List<Tuple<DateTime, DateTime>> non_dist_abort_txn_startAndEndTime;

        public WorkloadResult()
        {
            pact_dist_result = new BasicResult();
            pact_non_dist_result = new BasicResult();
            act_dist_result = new BasicACTResult();
            act_non_dist_result = new BasicACTResult();

            dist_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
            non_dist_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
            dist_abort_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
            non_dist_abort_txn_startAndEndTime = new List<Tuple<DateTime, DateTime>>();
        }

        public void SetTxnStartAndEndTime(
            List<Tuple<DateTime, DateTime>> dist_txn_startAndEndTime, List<Tuple<DateTime, DateTime>> non_dist_txn_startAndEndTime,
            List<Tuple<DateTime, DateTime>> dist_abort_txn_startAndEndTime, List<Tuple<DateTime, DateTime>> non_dist_abort_txn_startAndEndTime)
        {
            this.dist_txn_startAndEndTime = dist_txn_startAndEndTime;
            this.non_dist_txn_startAndEndTime = non_dist_txn_startAndEndTime;
            this.dist_abort_txn_startAndEndTime = dist_abort_txn_startAndEndTime;
            this.non_dist_abort_txn_startAndEndTime = non_dist_abort_txn_startAndEndTime;
        }

        public void SetTime(long startTime, long endTime)
        {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public void SetNumber(bool isDet, bool isDist, int numCommit, int numEmit, int numDeadlock, int numNotSerializable, int numNotSureSerializable, int numGrainMigration)
        {
            if (isDet)
            {
                if (isDist) pact_dist_result = new BasicResult(numCommit);
                else pact_non_dist_result = new BasicResult(numCommit);
            }
            else
            {
                if (isDist) act_dist_result = new BasicACTResult(numCommit, numEmit, numDeadlock, numNotSerializable, numNotSureSerializable, numGrainMigration);
                else act_non_dist_result = new BasicACTResult(numCommit, numEmit, numDeadlock, numNotSerializable, numNotSureSerializable, numGrainMigration);
            }
        }

        public void SetLatency(bool isDet, bool isDist, List<double> latency, List<double> registTxnTime, List<double> prepareTxnTime, List<double> executeTxnTime, List<double> commitTxnTime)
        {
            if (isDet)
            {
                if (isDist) pact_dist_result.latencies.SetLatency(latency, registTxnTime, prepareTxnTime, executeTxnTime, commitTxnTime);
                else pact_non_dist_result.latencies.SetLatency(latency, registTxnTime, prepareTxnTime, executeTxnTime, commitTxnTime);
            }
            else
            {
                if (isDist) act_dist_result.basic_result.latencies.SetLatency(latency, registTxnTime, prepareTxnTime, executeTxnTime, commitTxnTime);
                else act_non_dist_result.basic_result.latencies.SetLatency(latency, registTxnTime, prepareTxnTime, executeTxnTime, commitTxnTime);
            }
        }

        public void MergeData(WorkloadResult res)
        {
            startTime = Math.Min(startTime, res.startTime);
            endTime = Math.Max(endTime, res.endTime);

            pact_dist_result.MergeData(res.pact_dist_result);
            pact_non_dist_result.MergeData(res.pact_non_dist_result);

            act_dist_result.MergeData(res.act_dist_result);
            act_non_dist_result.MergeData(res.act_non_dist_result);

            dist_txn_startAndEndTime.AddRange(res.dist_txn_startAndEndTime);
            non_dist_txn_startAndEndTime.AddRange(res.non_dist_txn_startAndEndTime);
        }

        public PrintData GetPrintData()
        {
            var data = new PrintData();

            data.pact_dist_print.Calculate(pact_dist_result.numCommit, startTime, endTime);
            data.pact_non_dist_print.Calculate(pact_non_dist_result.numCommit, startTime, endTime);

            data.act_dist_print.Calculate(act_dist_result.basic_result.numCommit, startTime, endTime, 
                act_dist_result.numEmit, act_dist_result.numDeadlock, act_dist_result.numNotSerializable, act_dist_result.numNotSureSerializable, act_dist_result.numGrainMigration);
            data.act_non_dist_print.Calculate(act_non_dist_result.basic_result.numCommit, startTime, endTime,
                act_non_dist_result.numEmit, act_non_dist_result.numDeadlock, act_non_dist_result.numNotSerializable, act_non_dist_result.numNotSureSerializable, act_non_dist_result.numGrainMigration);

            var totalNumEmit = act_dist_result.numEmit + act_non_dist_result.numEmit;
            var totalNumCommit = act_dist_result.basic_result.numCommit + act_non_dist_result.basic_result.numCommit;
            data.act_total_abort = (totalNumEmit - totalNumCommit) * 100.0 / totalNumEmit;

            return data;
        }
    }

    public class BasicPrintData
    {
        public double throughput;

        public void Calculate(int numCommit, long startTime, long endTime)
        {
            throughput = numCommit * 1000.0 / (endTime - startTime);
        }
    }

    public class BasicACTPrintData
    {
        public BasicPrintData basic_data;
        public double abort;
        public double abort_deadlock;
        public double abort_notSerializable;
        public double abort_notSureSerializable;
        public double abort_grainMigration;

        public BasicACTPrintData()
        {
            basic_data = new BasicPrintData();
        }

        public void Calculate(int numCommit, long startTime, long endTime, int numEmit, int numDeadlock, int numNotSerializable, int numNotSureSerializable, int numGrainMigration)
        {
            basic_data.Calculate(numCommit, startTime, endTime);
            var numAbort = numEmit - numCommit;
            abort = numAbort * 100.0 / numEmit;
            abort_deadlock = numDeadlock * 100.0 / numAbort;
            abort_notSerializable = numNotSerializable * 100.0 / numAbort;
            abort_notSureSerializable = numNotSureSerializable * 100.0 / numAbort;
            abort_grainMigration = numGrainMigration * 100.0 / numAbort;
        }
    }

    public class PrintData
    {
        public BasicPrintData pact_dist_print;
        public BasicPrintData pact_non_dist_print;

        public double act_total_abort;
        public BasicACTPrintData act_dist_print;
        public BasicACTPrintData act_non_dist_print;

        public PrintData()
        {
            act_total_abort = 0;
            pact_dist_print = new BasicPrintData();
            pact_non_dist_print = new BasicPrintData();
            act_dist_print = new BasicACTPrintData();
            act_non_dist_print = new BasicACTPrintData();
        }
    }
}