using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using SmallBank.Interfaces;
using Utilities;

namespace SmallBank.Grains
{
    public class SnapperTransactionalAccountGrain : TransactionExecutionGrain<BankAccount>, ISnapperTransactionalAccountGrain
    {
        private readonly ILogger<SnapperTransactionalAccountGrain> logger;

        public SnapperTransactionalAccountGrain(ILogger<SnapperTransactionalAccountGrain> logger, ITransactionContextProviderFactory transactionContextProviderFactory) : base(logger, transactionContextProviderFactory)
        {
            this.logger = logger;
        }

        public async Task<TransactionResult> Init(TransactionContext context, FunctionInput functionInput)
        {
            var transactionInfo = functionInput.DestinationGrains.First();

            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.accountID = transactionInfo.DestinationGrain;
            myState.balance = transactionInfo.Value;

            this.logger.LogInformation("Balance {myStateBalance}", this.GrainReference, myState.balance);

            return new TransactionResult();
        }

        public async Task<TransactionResult> MultiTransfer(TransactionContext context, FunctionInput functionInput)
        {
            var destinationGrains = functionInput.DestinationGrains;
            var myState = await this.GetState(context, AccessMode.ReadWrite);

            myState.balance -= destinationGrains.Select(g => g.Value).Sum();

            var depositTasks = new List<Task>();
            foreach (var transactionInfo in destinationGrains)
            {
                var depositFunctionInput = new FunctionInput() { DestinationGrains = new List<TransactionInfo>() { transactionInfo } };

                if (transactionInfo.DestinationGrain != myState.accountID)
                {
                    var funcCall = new FunctionCall("Deposit", depositFunctionInput, typeof(SnapperTransactionalAccountGrain));
                    var depositTask = this.CallGrain(context, transactionInfo.DestinationGrain, "SmallBank.Grains.SnapperTransactionalAccountGrain", funcCall);

                    depositTasks.Add(depositTask);
                }
                // This logic is weird, one of the recipients could be it self
                else
                {
                    depositTasks.Add(Deposit(context, depositFunctionInput));
                }
            }

            await Task.WhenAll(depositTasks);

            return new TransactionResult();
        }

        public async Task<TransactionResult> Deposit(TransactionContext context, FunctionInput functionInput)
        {
            var transactionInfo = functionInput.DestinationGrains.First();
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.balance += transactionInfo.Value;

            return new TransactionResult();
        }

        public async Task<TransactionResult> Balance(TransactionContext context, FunctionInput functionInput)
        {
            var myState = await this.GetState(context, AccessMode.Read);

            return new TransactionResult(myState.balance);
        }
    }
}