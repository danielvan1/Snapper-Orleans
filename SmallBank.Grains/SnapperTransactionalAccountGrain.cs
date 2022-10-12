using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionBroadcasting;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Implementation.TransactionExecution.TransactionExecution;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using SmallBank.Interfaces;
using Utilities;

namespace SmallBank.Grains
{
    public class SnapperTransactionalAccountGrain : TransactionExecutionGrain<BankAccount>, ISnapperTransactionalAccountGrain
    {
        private readonly ILogger<SnapperTransactionalAccountGrain> logger;

        public SnapperTransactionalAccountGrain(ILogger<SnapperTransactionalAccountGrain> logger,
                                                ITransactionContextProviderFactory transactionContextProviderFactory,
                                                ITransactionBroadCasterFactory transactionBroadCasterFactory,
                                                IDeterministicTransactionExecutorFactory deterministicTransactionExecutorFactory) : base(logger, transactionContextProviderFactory, transactionBroadCasterFactory, deterministicTransactionExecutorFactory, "SmallBank.Grains.SnapperTransactionalAccountGrain")
        {
            this.logger = logger;
        }

        public async Task<TransactionResult> Init(TransactionContext context, FunctionInput functionInput)
        {
            var accountID = functionInput.DestinationGrains.First();
            BankAccount myState =  await this.GetState(context, AccessMode.ReadWrite);
            myState.accountID = accountID.DestinationGrain;
            myState.balance = accountID.Value;

            this.logger.LogInformation("Balance {myStateBalance}", this.GrainReference, myState.balance);

            return new TransactionResult();
        }

        public async Task<TransactionResult> MultiTransfer(TransactionContext context, FunctionInput functionInput)
        {
            var toAccounts = functionInput.DestinationGrains;
            var myState = await this.GetState(context, AccessMode.ReadWrite);
            myState.balance -= toAccounts.Select(acc => acc.Value).Sum();

            var task = new List<Task>();

            foreach (var accountID in toAccounts)
            {
                this.logger.LogInformation("MyState account: {id} and ToAccount: {toId}", this.GrainReference, myState.accountID, accountID);
                if (accountID.DestinationGrain != myState.accountID)
                {
                    var funcCall = new FunctionCall("Deposit", functionInput, typeof(SnapperTransactionalAccountGrain));
                    var t = this.CallGrain(context, accountID.DestinationGrain, "SmallBank.Grains.SnapperTransactionalAccountGrain", funcCall);
                    task.Add(t);
                }
                // This logic is weird, one of the recipients could be it self
                else
                {
                    var herp = new FunctionInput()
                    {
                        DestinationGrains = new List<TransactionInfo>()
                        {
                            accountID
                        }

                    };

                    task.Add(Deposit(context, herp));
                }
            }
            await Task.WhenAll(task);

            return new TransactionResult();
        }

        public async Task<TransactionResult> Deposit(TransactionContext context, FunctionInput functionInput)
        {
            var accountID = functionInput.DestinationGrains.First();
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.balance += accountID.Value;

            return new TransactionResult();
        }

        public async Task<TransactionResult> Balance(TransactionContext context, FunctionInput functionInput)
        {
            var myState = await this.GetState(context, AccessMode.Read);

            return new TransactionResult(myState.balance);
        }
    }
}