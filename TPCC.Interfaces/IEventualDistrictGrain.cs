using Utilities;
using Orleans.Concurrency;

namespace TPCC.Interfaces
{
    public interface IEventualDistrictGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
