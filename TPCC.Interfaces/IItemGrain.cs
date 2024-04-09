using Utilities;
using Concurrency.Interface.TransactionExecution;

namespace TPCC.Interfaces
{
    public interface IItemGrain : ITransactionExecutionGrain
    {
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> GetItemsPrice(MyTransactionContext context, object funcInput);   // an invalid I_ID get price -1
    }
}