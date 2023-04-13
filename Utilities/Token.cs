using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class BasicToken
    {
        public long lastEmitBid;
        public long lastEmitTid;
        public Guid lastCoordID;
        public long highestCommittedBid;
        public bool isLastEmitBidGlobal;
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<string, long> lastBidPerService;

        // this info is only used for local coordinators
        public Dictionary<string, long> lastGlobalBidPerGrain;   // grainID, the global bid of the latest emitted local batch

        public BasicToken()
        {
            lastEmitBid = -1;
            lastEmitTid = -1;
            lastCoordID = Guid.Empty;
            highestCommittedBid = -1;
            isLastEmitBidGlobal = false;
            lastBidPerService = new Dictionary<string, long>();
            lastGlobalBidPerGrain = new Dictionary<string, long>();
        }
    }

    [Serializable]
    public class LocalToken : BasicToken
    {
        // for global info
        public long lastEmitGlobalBid;
        public long highestCommittedGlobalBid;

        // this is for grain migration (GC)
        public long highestCommittedLocalBidOnGrain;
        public long highestCommittedGlobalBidOnGrain;

        public LocalToken() : base()
        {
            lastEmitGlobalBid = -1;
            highestCommittedGlobalBid = -1;
            highestCommittedLocalBidOnGrain = -1;
            highestCommittedGlobalBidOnGrain = -1;
        }
    }
}