using Utilities;
using Concurrency.Interface.TransactionExecution;

namespace TPCC.Interfaces
{
    public interface ICustomerGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext ctx, object funcInput);
        Task<TransactionResult> NewOrder(MyTransactionContext ctx, object funcInput);
    }
}