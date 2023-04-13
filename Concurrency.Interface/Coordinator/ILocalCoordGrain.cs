using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Concurrency;
using System;

namespace Concurrency.Interface.Coordinator
{
    public interface ILocalCoordGrain : IGrainWithGuidKey
    {
        Task Init(bool optimizeCommit);
        // <bid, tid, highestCommittedGlobalBidOnGrain, highestCommittedLocalBidOnGrain, highestCommittedGlobalBid, highestCommittedLocalBid>
        Task<Tuple<long, long, long, long, long, long>> NewRegularLocalPACT(List<string> grainAccessInfo);
        Task<Tuple<long, long, long, long, long, long>> NewTemporaryLocalPACT(long globalBid, long globalTid, List<string> grainAccessInfo);

        Task<Tuple<long, long, long>> NewACT();   // <tid, highestCommittedGlobalBid, highestCommittedLocalBid>
        Task<long> GetHighestCommittedLocalBid();

        [OneWay]
        Task PassToken(LocalToken token);
        [OneWay]
        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);

        Task AckGlobalBatchCommit(long globalBid);
        
        Task ReceiveBatchSchedule(SubBatch batch);

        Task CheckGC();
        
        // this is only for ACT hybrid processing
        Task<long> NewWaitBatchCommit(long bid);
    }
}