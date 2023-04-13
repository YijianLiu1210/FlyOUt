using Orleans;
using System;
using System.Collections.Generic;
using Utilities;
using System.Threading.Tasks;

namespace Concurrency.Interface.GrainPlacement
{
    public interface IGrainPlacementManager : IGrainWithGuidKey
    {
        Task Init(int numLocalSilo);
        Task<Tuple<TransactionContext, long, long>> NewTransaction(List<Guid> grainList);  // <cxt, highestCommittedGlobalBid, highestCommittedLocalBid>

        Task CheckGC();

        Task<Tuple<long, long>> FreezeGrain(Guid grainID);

        Task UnFreezeGrain(Guid grainID);
    }
}
