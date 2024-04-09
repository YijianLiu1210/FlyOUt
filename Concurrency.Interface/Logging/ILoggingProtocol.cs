using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Utilities;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol
    {
        Task Init(bool isLoggingEnabled, bool isGlobalSilo, int siloID, int numLocalSilo, bool hierarchicalCoord);
        bool IsLoggingEnabled();
        
        // for ACT
        Task CoordPrepare(GrainID coordID, long tid, HashSet<GrainID> participateGrains);
        Task Prepare(GrainID grainID, long tid, GrainID coordID, byte[] state, DateTime timestamp);
        Task CoordCommit(GrainID coordID, long tid);
        Task Commit(GrainID grainID, long tid);

        // for PACT batch
        Task GlobalBatchInfo(Guid globalCoordID, long globalBid, HashSet<Guid> participateLocalCoords);
        Task LocalBatchInfo(Guid localCoordID, long localBid, long globalBid, Guid globalCoordID, HashSet<GrainID> participateGrains);
        Task LocalBatchComplete(GrainID grainID, long localBid, Guid localCoordID, byte[] state, DateTime timestamp);
        Task LocalBatchCommit(Guid localCoordID, long localBid);

        // for grain migration
        Task<byte[]> GetLastCommittedGrainStateFromLog(GrainID grainID, long lastCommittedLocalBid);
    }
}