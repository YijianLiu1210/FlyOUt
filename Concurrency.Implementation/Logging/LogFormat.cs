using MessagePack;
using System;
using System.Collections.Generic;
using Utilities;

namespace Concurrency.Implementation.Logging
{
    [MessagePackObject]
    public class LogFormat
    {
        [Key(0)]
        public readonly LogContentType logType;
        [Key(1)]
        public readonly byte[] logContent;

        public LogFormat(LogContentType logType, byte[] logContent)
        {
            this.logType = logType;
            this.logContent = logContent;
        }
    }

    [MessagePackObject]
    public class CoordPrepareLog
    {
        [Key(0)]
        public readonly Guid coordID;
        [Key(1)]
        public readonly long tid;
        [Key(2)]
        public readonly HashSet<Guid> participateGrains;

        public CoordPrepareLog(Guid coordID, long tid, HashSet<Guid> participateGrains)
        {
            this.coordID = coordID;
            this.tid = tid;
            this.participateGrains = participateGrains;
        }
    }

    [MessagePackObject]
    public class PrepareLog
    {
        [Key(0)]
        public readonly DateTime timestamp;
        [Key(1)]
        public readonly Guid grainID;
        [Key(2)]
        public readonly long tid;
        [Key(3)]
        public readonly Guid coordID;
        [Key(4)]
        public readonly byte[] state;

        public PrepareLog(DateTime timestamp, Guid grainID, long tid, Guid coordID, byte[] state)
        {
            this.timestamp = timestamp;
            this.grainID = grainID;
            this.tid = tid;
            this.coordID = coordID;
            this.state = state;
        }
    }

    [MessagePackObject]
    public class CoordCommitLog
    {
        [Key(0)]
        public readonly Guid coordID;
        [Key(1)]
        public readonly long tid;

        public CoordCommitLog(Guid coordID, long tid)
        {
            this.coordID = coordID;
            this.tid = tid;
        }
    }

    [MessagePackObject]
    public class CommitLog
    {
        [Key(0)]
        public readonly Guid grainID;
        [Key(1)]
        public readonly long tid;

        public CommitLog(Guid grainID, long tid)
        {
            this.grainID = grainID;
            this.tid = tid;
        }
    }

    [MessagePackObject]
    public class GlobalBatchInfoLog
    {
        [Key(0)]
        public readonly Guid globalCoordID;
        [Key(1)]
        public readonly long globalBid;
        [Key(2)]
        public readonly HashSet<Guid> participateLocalCoords;

        public GlobalBatchInfoLog(Guid globalCoordID, long globalBid, HashSet<Guid> participateLocalCoords)
        {
            this.globalCoordID = globalCoordID;
            this.globalBid = globalBid;
            this.participateLocalCoords = participateLocalCoords;
        }
    }

    [MessagePackObject]
    public class LocalBatchInfoLog
    {
        [Key(0)]
        public readonly Guid localCoordID;
        [Key(1)]
        public readonly long localBid;
        [Key(2)]
        public readonly long globalBid;
        [Key(3)]
        public readonly Guid globalCoordID;
        [Key(4)]
        public readonly HashSet<Guid> participateGrains;

        public LocalBatchInfoLog(Guid localCoordID, long localBid, long globalBid, Guid globalCoordID, HashSet<Guid> participateGrains)
        { 
            this.localCoordID = localCoordID;
            this.localBid = localBid;
            this.globalBid = globalBid;
            this.globalCoordID = globalCoordID;
            this.participateGrains = participateGrains;
        }
    }

    [MessagePackObject]
    public class LocalBatchCompleteLog
    {
        [Key(0)]
        public readonly DateTime timestamp;
        [Key(1)]
        public readonly Guid grainID;
        [Key(2)]
        public readonly long localBid;
        [Key(3)]
        public readonly Guid localCoordID;
        [Key(4)]
        public readonly byte[] state;

        public LocalBatchCompleteLog(DateTime timestamp, Guid grainID, long localBid, Guid localCoordID, byte[] state)
        {
            this.timestamp = timestamp;
            this.grainID = grainID;
            this.localBid = localBid;
            this.localCoordID = localCoordID;
            this.state = state;
        }
    }

    [MessagePackObject]
    public class LocalBatchCommitLog
    {
        [Key(0)]
        public readonly Guid localCoordID;
        [Key(1)]
        public readonly long localBid;

        public LocalBatchCommitLog(Guid localCoordID, long localBid)
        { 
            this.localCoordID = localCoordID;
            this.localBid = localBid;
        }
    }
}