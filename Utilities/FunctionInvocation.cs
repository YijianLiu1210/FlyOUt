using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class GrainMigrationRequestResult
    {
        public double informGrainTime;
        public double freezeGrainTime;
        public double prepareDeactivateTime;
        public double waitTxnCommitTime;      // prepare 1
        public double getCommittedStateTime;    // prepare 2
        public double deserializeStateTime;     // prepare 3
        public double updateRedisTime;
        public double updateCacheTime;
        public double deactivateTime;
        public double activateTime;
        public double unfreezeTime;
    }

    [Serializable]
    public class TransactionResult
    {
        public object resultObj;

        // only for ACT
        public bool exception;
        public bool Exp_Deadlock;
        public bool Exp_Serializable;
        public bool Exp_NotSureSerializable;
        public bool Exp_GrainMigration;

        // investigate PACT breakdown latency
        public double registTime;     // receive txn request  ==> receive basic context info
        public double prepareTime;    // receive context info ==> start execute txn
        public double executeTime;    // start execute txn    ==> finish execute txn
        public double commitTime;     // finish execute txn   ==> batch has committed

        public TransactionResult(object resultObj = null)
        {
            exception = false;
            Exp_Deadlock = false;
            Exp_Serializable = false;
            Exp_NotSureSerializable = false;
            Exp_GrainMigration = false;
            this.resultObj = resultObj;
        }
    }

    [Serializable]
    public class OpOnGrain
    {
        public bool isNoOp;
        public bool isReadonly;

        public OpOnGrain(bool isNoOp, bool isReadonly)
        {
            this.isNoOp = isNoOp;
            this.isReadonly = isReadonly;
        }
    }

    [Serializable]
    public class BasicFuncResult
    {
        public object resultObj;
        public bool isNoOpOnGrain;
        public bool isReadOnlyOnGrain;
        public Dictionary<Guid, OpOnGrain> grainOpInfo;   // <grainID, operations performed on the grains>

        public BasicFuncResult()
        {
            isNoOpOnGrain = true;
            isReadOnlyOnGrain = true;
            grainOpInfo = new Dictionary<Guid, OpOnGrain>();
        }

        public void SetResultObj(object resultObj)
        {
            this.resultObj = resultObj;
        }

        public void MergeGrainOpInfo(BasicFuncResult res)
        {
            foreach (var item in res.grainOpInfo)
            {
                if (grainOpInfo.ContainsKey(item.Key) == false)
                    grainOpInfo.Add(item.Key, item.Value);
                else
                {
                    var isReadOnly = grainOpInfo[item.Key].isReadonly && item.Value.isReadonly;
                    var isNoOp = grainOpInfo[item.Key].isNoOp && item.Value.isNoOp;
                    grainOpInfo[item.Key] = new OpOnGrain(isNoOp, isReadOnly);
                }
            }
        }
    }

    [Serializable]
    public class NonDetScheduleInfo
    {
        public long maxBeforeBid;
        public long minAfterBid;
        public bool isAfterComplete;

        public NonDetScheduleInfo()
        {
            maxBeforeBid = -1;
            minAfterBid = long.MaxValue;
            isAfterComplete = true;
        }

        public NonDetScheduleInfo(long maxBeforeBid, long minAfterBid, bool isAfterComplete)
        {
            this.maxBeforeBid = maxBeforeBid;
            this.minAfterBid = minAfterBid;
            this.isAfterComplete = isAfterComplete;
        }
    }

    [Serializable]
    public class NonDetFuncResult : BasicFuncResult
    {
        public bool exception;
        public bool Exp_Deadlock;
        public bool Exp_GrainMigration;

        // this info is used to check global serializability
        public NonDetScheduleInfo globalScheduleInfo;
        public Dictionary<string, NonDetScheduleInfo> scheduleInfoPerSilo;     // <silo ID, scheule info>
        public CommitInfo commitInfo;
        
        public NonDetFuncResult() : base()
        {
            exception = false;
            Exp_Deadlock = false;
            Exp_GrainMigration = false;
            globalScheduleInfo = new NonDetScheduleInfo();
            scheduleInfoPerSilo = new Dictionary<string, NonDetScheduleInfo>();
            commitInfo = new CommitInfo();
        }

        public void MergeFuncResult(NonDetFuncResult res)
        {
            exception |= res.exception;
            Exp_Deadlock |= res.Exp_Deadlock;
            Exp_GrainMigration |= res.Exp_GrainMigration;

            MergeGrainOpInfo(res);

            MergeBeforeAfterGlobalInfo(res.globalScheduleInfo);

            foreach (var info in res.scheduleInfoPerSilo)
                MergeBeforeAfterLocalInfo(info.Value, info.Key);

            commitInfo.MergeCommitInfo(res.commitInfo);
        }

        public void MergeBeforeAfterLocalInfo(NonDetScheduleInfo newLocalInfo, string mySiloAddress)
        {
            if (scheduleInfoPerSilo.ContainsKey(mySiloAddress) == false)
                scheduleInfoPerSilo.Add(mySiloAddress, newLocalInfo);
            else
            {
                var myLocalInfo = scheduleInfoPerSilo[mySiloAddress];
                myLocalInfo.maxBeforeBid = Math.Max(myLocalInfo.maxBeforeBid, newLocalInfo.maxBeforeBid);
                myLocalInfo.minAfterBid = Math.Min(myLocalInfo.minAfterBid, newLocalInfo.minAfterBid);
                myLocalInfo.isAfterComplete &= newLocalInfo.isAfterComplete;
            }
        }

        public void MergeBeforeAfterGlobalInfo(NonDetScheduleInfo newGlobalInfo)
        {
            globalScheduleInfo.maxBeforeBid = Math.Max(globalScheduleInfo.maxBeforeBid, newGlobalInfo.maxBeforeBid);
            globalScheduleInfo.minAfterBid = Math.Min(globalScheduleInfo.minAfterBid, newGlobalInfo.minAfterBid);
            globalScheduleInfo.isAfterComplete &= newGlobalInfo.isAfterComplete;
        }
    }

    [Serializable]
    public class FunctionCall
    {
        public readonly string funcName;
        public readonly object funcInput;
        public readonly Type className;

        public FunctionCall(string funcName, object funcInput, Type className)
        {
            this.funcName = funcName;
            this.funcInput = funcInput;
            this.className = className;
        }
    }
}