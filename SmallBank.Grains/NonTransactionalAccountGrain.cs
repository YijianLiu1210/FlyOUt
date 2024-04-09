using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Concurrency;
using Concurrency.Implementation.GrainPlacement;

namespace SmallBank.Grains
{
    using MultiTransferInput = Tuple<int, List<Guid>>;  // money, List<to account>

    [Reentrant]
    [SnapperGrainPlacementStrategy(GrainType.UserGrain)]
    public class NonTransactionalAccountGrain : Orleans.Grain, INonTransactionalAccountGrain
    {
        BankAccount state;

        Task<TransactionResult> Init(object funcInput)
        {
            var accountID = (Guid)funcInput;
            state = new BankAccount(accountID, int.MaxValue);
            return Task.FromResult(new TransactionResult());
        }

        async Task<TransactionResult> MultiTransfer(object funcInput)
        {
            var input = (MultiTransferInput)funcInput;
            var money = input.Item1;
            var toAccounts = input.Item2;

            state.balance -= money * toAccounts.Count;

            var task = new List<Task>();
            foreach (var accountID in toAccounts)
            {
                if (accountID != state.accountID)
                {
                    var grain = GrainFactory.GetGrain<INonTransactionalAccountGrain>(accountID);
                    var t = grain.StartTransaction("Deposit", money);
                    task.Add(t);
                }
                else task.Add(Deposit(money));
            }
            await Task.WhenAll(task);
            return new TransactionResult();
        }

        Task<TransactionResult> Deposit(object funcInput)
        {
            var money = (int)funcInput;
            state.balance += money;
            return Task.FromResult(new TransactionResult());
        }

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            TxnType fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case TxnType.Init:
                    return Init(funcInput);
                case TxnType.MultiTransfer:
                    return MultiTransfer(funcInput);
                case TxnType.Deposit:
                    return Deposit(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}