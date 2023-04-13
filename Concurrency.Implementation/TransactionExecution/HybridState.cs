using System;
using System.Threading.Tasks;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.TransactionExecution.Nondeterministic;
using Concurrency.Implementation.TransactionExecution.Nondeterministic;

namespace Concurrency.Implementation.TransactionExecution
{
    public class HybridState<TState> : ITransactionalState<TState> where TState : ICloneable, new()
    {
        TState committedState;
        INonDetTransactionalState<TState> nonDetStateManager;
        
        // when execution grain is initialized, its hybrid state is initialized
        public HybridState(TState state)
        {
            committedState = state;
            nonDetStateManager = new S2PLTransactionalState<TState>(state);
        }

        public void SetState(TState state) => committedState = state;

        public void CheckGC() => nonDetStateManager.CheckGC();
       
        public TState GetCommittedState() => committedState;

        public async Task<TState> NonDetRead(long tid) => await nonDetStateManager.Read(tid, committedState);
        
        public async Task<TState> NonDetReadWrite(long tid) => await nonDetStateManager.ReadWrite(tid, committedState);
        
        public Task<bool> Prepare(long tid, bool isReader) => nonDetStateManager.Prepare(tid, isReader);

        public void Commit(long tid) => nonDetStateManager.Commit(tid, ref committedState);
        
        public void Abort(long tid) => nonDetStateManager.Abort(tid);
        
        public TState GetPreparedState(long tid) => nonDetStateManager.GetPreparedState(tid);
    }
}