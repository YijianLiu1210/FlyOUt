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
        Task<Tuple<MyTransactionContext, long, long>> NewTransaction(List<GrainID> grainList);  // <cxt, highestCommittedGlobalBid, highestCommittedLocalBid>

        Task CheckGC();

        Task<Tuple<long, long>> FreezeGrain(GrainID grainID);

        Task UnFreezeGrain(GrainID grainID);
    }
}
