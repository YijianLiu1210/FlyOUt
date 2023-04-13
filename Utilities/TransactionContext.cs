using System;

namespace Utilities
{
    [Serializable]
    public class TransactionContext
    {
        // only for PACT
        public long localBid;
        public long localTid;
        public readonly long globalBid;

        // for global PACT and all ACT
        public readonly long globalTid;

        // only for ACT: the grain who starts the ACT
        public readonly Guid nonDetCoordID;

        /// <summary> This constructor is only for local PACT </summary>
        public TransactionContext(long localBid, long localTid)
        {
            this.localBid = localBid;
            this.localTid = localTid;
            globalBid = -1;
            globalTid = -1;
            nonDetCoordID = Guid.Empty;
        }

        /// <summary> This constructor is only for global PACT </summary>
        public TransactionContext(long localBid, long localTid, long globalBid, long globalTid)
        {
            this.localBid = localBid;
            this.localTid = localTid;
            this.globalBid = globalBid;
            this.globalTid = globalTid;
            nonDetCoordID = Guid.Empty;
        }

        /// <summary> This constructor is only for ACT </summary>
        public TransactionContext(long globalTid, Guid nonDetCoordID)
        {
            localBid = -1;
            localTid = -1;
            globalBid = -1;
            this.globalTid = globalTid;
            this.nonDetCoordID = nonDetCoordID;
        }
    }
}