using Utilities;
using Concurrency.Interface.TransactionExecution;

namespace TPCC.Interfaces
{
    public interface IDistrictGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext ctx, object funcInput);
        Task<TransactionResult> GetDTax(MyTransactionContext ctx, object funcInput);
    }
}