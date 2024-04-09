using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Logging;
using Concurrency.Interface.GrainPlacement;
using StackExchange.Redis;
using System.Diagnostics;

namespace SmallBank.Grains
{
    using MultiTransferInput = Tuple<int, List<GrainID>>;  // money, List<to account>

    public class SnapperTransactionalAccountGrain : TransactionExecutionGrain<BankAccount>, ISnapperTransactionalAccountGrain
    {
        public SnapperTransactionalAccountGrain(ILoggingProtocol log, IGrainPlacementCache grainPlacementInfo, IConnectionMultiplexer redis) : base(log, grainPlacementInfo, redis)
        {
        }

        public Task<string> GetSiloAddress() => Task.FromResult(this.RuntimeIdentity);

        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
        {
            var accountID = (Guid)funcInput;
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.accountID = accountID;
            myState.balance = int.MaxValue;
            return new TransactionResult();
        }

        public async Task<TransactionResult> MultiTransfer(MyTransactionContext context, object funcInput)
        {
            var input = (MultiTransferInput)funcInput;
            var money = input.Item1;
            var toAccounts = input.Item2;
            var myState = await GetState(context, AccessMode.ReadWrite);

            myState.balance -= money * toAccounts.Count;

            var task = new List<Task>();
            foreach (var accountID in toAccounts)
            {
                Debug.Assert(accountID.id != myState.accountID);
                var funcCall = new FunctionCall("Deposit", money, typeof(SnapperTransactionalAccountGrain));
                var t = CallGrain(context, accountID.id, accountID.className, funcCall);
                task.Add(t);
            }
            await Task.WhenAll(task);
            return new TransactionResult();
        }
       
        public async Task<TransactionResult> Deposit(MyTransactionContext context, object funcInput)
        {
            var money = (int)funcInput;
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.balance += money;
            return new TransactionResult();
        }

        public async Task<TransactionResult> Balance(MyTransactionContext context, object funcInput)
        {
            var myState = await GetState(context, AccessMode.Read);
            return new TransactionResult(myState.balance);
        }
    }
}