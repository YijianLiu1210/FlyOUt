using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithGuidKey
    {
        // PACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<Guid> grainAccessInfo);
        Task<Tuple<object, DateTime>> ExecuteDet(FunctionCall call, TransactionContext ctx);
        Task ReceiveBatchSchedule(LocalSubBatch batch);
        Task AckBatchCommit(long localBid, long globalBid);

        // ACT
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
        Task<Tuple<NonDetFuncResult, DateTime>> ExecuteNonDet(FunctionCall call, TransactionContext ctx);
        Task<bool> Prepare(long tid, bool isReader);
        Task Commit(long tid, long maxBeforeLocalBid, long maxBeforeGlobalBid);
        Task Abort(long tid);

        // for grain migration
        Task StartMigration();
        //Task<byte[]> PrepareDeactivation(long localBid, long globalBid);
        Task<Tuple<double, double, double, byte[]>> PrepareDeactivation(long localBid, long globalBid);
        Task DeactivateGrain();
        Task ActivateGrain(byte[] data = null);

        Task CheckGC();
    }
}