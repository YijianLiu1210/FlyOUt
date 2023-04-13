using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol
    {
        Task Init(bool isLoggingEnabled, bool isGlobalSilo, int siloID, int numLocalSilo, bool hierarchicalCoord);
        bool IsLoggingEnabled();
        
        // for ACT
        Task CoordPrepare(Guid coordID, long tid, HashSet<Guid> participateGrains);
        Task Prepare(Guid grainID, long tid, Guid coordID, byte[] state, DateTime timestamp);
        Task CoordCommit(Guid coordID, long tid);
        Task Commit(Guid grainID, long tid);

        // for PACT batch
        Task GlobalBatchInfo(Guid globalCoordID, long globalBid, HashSet<Guid> participateLocalCoords);
        Task LocalBatchInfo(Guid localCoordID, long localBid, long globalBid, Guid globalCoordID, HashSet<Guid> participateGrains);
        Task LocalBatchComplete(Guid grainID, long localBid, Guid localCoordID, byte[] state, DateTime timestamp);
        Task LocalBatchCommit(Guid localCoordID, long localBid);

        // for grain migration
        Task<byte[]> GetLastCommittedGrainStateFromLog(Guid grainID, long lastCommittedLocalBid);
    }
}