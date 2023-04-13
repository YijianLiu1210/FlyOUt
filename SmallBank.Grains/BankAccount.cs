using System;
using Concurrency.Interface.TransactionExecution;
using MessagePack;

namespace SmallBank.Grains
{
    [MessagePackObject]
    public class BankAccount : ICloneable, IPrintable
    {
        [Key(0)]
        public Guid accountID;
        [Key(1)]
        public double balance;

        public BankAccount(Guid accountID, double balance)
        { 
            this.accountID = accountID;
            this.balance = balance;
        }

        public BankAccount() { }

        public object Clone() => new BankAccount(accountID, balance);

        public string PrintState() => accountID.ToString();
    }
}