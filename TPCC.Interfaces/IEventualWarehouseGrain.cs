using Orleans.Concurrency;
using Utilities;

namespace TPCC.Interfaces
{
    public interface IEventualWarehouseGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
