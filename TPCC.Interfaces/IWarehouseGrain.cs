using Utilities;
using Concurrency.Interface.TransactionExecution;

namespace TPCC.Interfaces
{
    public interface IWarehouseGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> GetWTax(MyTransactionContext context, object funcInput);
    }
}