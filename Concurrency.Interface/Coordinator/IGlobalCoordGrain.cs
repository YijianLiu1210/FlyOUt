using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Orleans.Concurrency;

namespace Concurrency.Interface.Coordinator
{
    public interface IGlobalCoordGrain : IGrainWithGuidKey
    {
        Task Init();
        Task SetBatchSize(int numLocalSilo, int batchSizeInMSecsBasic);
        [OneWay]
        Task PassToken(BasicToken token);

        Task<Tuple<long, long>> NewACT();
        Task<long> GetHighestCommittedGlobalBid();

        // <global bid, global tid, <silo name, the selected local coord id>>
        Task<Tuple<long, long, Dictionary<string, Guid>>> NewGlobalPACT(List<string> siloList);
        [OneWay]
        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);

        Task CheckGC();
    }
}