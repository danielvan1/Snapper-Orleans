using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Implementation.TransactionExecution.TransactionExecution;
using Microsoft.Extensions.Logging;
using SmallBank.Interfaces;
using Utilities;

namespace SmallBank.Grains
{
    using MultiTransferInput = Tuple<int, List<Tuple<int, string>>>;  // money, List<to account>

    public class SnapperTransactionalAccountGrain : TransactionExecutionGrain<BankAccount>, ISnapperTransactionalAccountGrain
    {
        private readonly ILogger<SnapperTransactionalAccountGrain> logger;

        public SnapperTransactionalAccountGrain(ILogger<SnapperTransactionalAccountGrain> logger,
                                                ITransactionContextProviderFactory transactionContextProviderFactory,
                                                IDeterministicTransactionExecutorFactory deterministicTransactionExecutorFactory) : base(logger, transactionContextProviderFactory, deterministicTransactionExecutorFactory)
        {
            this.logger = logger;
        }

        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            var accountID = (Tuple<int, string>)funcInput;
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.accountID = accountID;
            myState.balance = 10000;
            this.logger.LogInformation("Balance {myStateBalance}", this.GrainReference, myState.balance);
            return new TransactionResult();
        }

        public async Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput)
        {
            var input = (MultiTransferInput)funcInput;
            var money = input.Item1;
            var toAccounts = input.Item2;
            var myState = await GetState(context, AccessMode.ReadWrite);

            myState.balance -= money * toAccounts.Count;

            var task = new List<Task>();
            foreach (var accountID in toAccounts)
            {
                this.logger.LogInformation("MyState account: {id} and ToAccount: {toId}", this.GrainReference, myState.accountID, accountID);
                if (accountID != myState.accountID)
                {
                    var funcCall = new FunctionCall("Deposit", money, typeof(SnapperTransactionalAccountGrain));
                    var t = this.CallGrain(context, accountID, "SmallBank.Grains.SnapperTransactionalAccountGrain", funcCall);
                    task.Add(t);
                }
                // This logic is weird, one of the recipients could be it self
                else
                {
                    task.Add(Deposit(context, money));
                }
            }
            await Task.WhenAll(task);
            return new TransactionResult();
        }

        public async Task<TransactionResult> Deposit(TransactionContext context, object funcInput)
        {
            var money = (int)funcInput;
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.balance += money;
            return new TransactionResult();
        }

        public async Task<TransactionResult> Balance(TransactionContext context, object funcInput)
        {
            var myState = await GetState(context, AccessMode.Read);
            return new TransactionResult(myState.balance);
        }
    }
}