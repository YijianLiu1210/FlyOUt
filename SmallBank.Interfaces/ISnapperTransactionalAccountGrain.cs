using Utilities;
using Concurrency.Interface.TransactionExecution;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface ISnapperTransactionalAccountGrain : ITransactionExecutionGrain
    {
        Task<string> GetSiloAddress();
        Task<TransactionResult> Init(MyTransactionContext context, object funcInput);
        Task<TransactionResult> Balance(MyTransactionContext context, object funcInput);
        Task<TransactionResult> MultiTransfer(MyTransactionContext context, object funcInput);
        Task<TransactionResult> Deposit(MyTransactionContext context, object funcInput);
    }
}