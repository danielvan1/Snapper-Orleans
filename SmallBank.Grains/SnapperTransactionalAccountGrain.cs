using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation;
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
                                                IDeterministicTransactionExecutorFactory deterministicTransactionExecutorFactory,
                                                IIdHelper idHelper,
                                                List<string> region)
                                                : base(logger, transactionContextProviderFactory, transactionBroadCasterFactory, deterministicTransactionExecutorFactory, idHelper, "SmallBank.Grains.SnapperTransactionalAccountGrain",  region)
        {
            this.logger = logger;
        }

        public async Task<TransactionResult> Init(TransactionContext context, FunctionInput functionInput)
        {
            var accountID = functionInput.DestinationGrains.First();
            BankAccount myState =  await this.GetState(context, AccessMode.ReadWrite);
            myState.accountID = accountID.DestinationGrain;
            myState.Balance = accountID.Value;

            this.logger.LogInformation("Balance {myStateBalance}", this.GrainReference, myState.Balance);

            return new TransactionResult();
        }

        public async Task<TransactionResult> MultiTransfer(TransactionContext context, FunctionInput functionInput)
        {
            var toAccounts = functionInput.DestinationGrains;
            var myState = await this.GetState(context, AccessMode.ReadWrite);
            myState.Balance -= toAccounts.Select(acc => acc.Value)
                                         .Sum();

            var tasks = new List<Task>();

            foreach (var accountID in toAccounts)
            {
                this.logger.LogInformation("MyState account: {id} and ToAccount: {toId}", this.GrainReference, myState.accountID, accountID);
                if (accountID.DestinationGrain != myState.accountID)
                {
                    var funcCall = new FunctionCall("Deposit", functionInput, typeof(SnapperTransactionalAccountGrain));
                    var t = this.CallGrain(context, accountID.DestinationGrain, "SmallBank.Grains.SnapperTransactionalAccountGrain", funcCall);
                    tasks.Add(t);
                }
                // This logic is weird, one of the recipients could be it self
                else
                {
                    var task = new FunctionInput()
                    {
                        DestinationGrains = new List<TransactionInfo>() { accountID }
                    };

                    tasks.Add(Deposit(context, task));
                }
            }

            await Task.WhenAll(tasks);

            this.logger.LogInformation("Done with multi transfer for context: {context}", this.GrainReference, context);

            return new TransactionResult()
            {
                Result = myState.Balance
            };
        }

        public async Task<TransactionResult> Deposit(TransactionContext context, FunctionInput functionInput)
        {
            var accountID = functionInput.DestinationGrains.First();
            var myState = await this.GetState(context, AccessMode.ReadWrite);
            this.logger.LogInformation("Going to deposit on this account: {accountId}", this.GrainReference, accountID);
            myState.Balance += accountID.Value;

            return new TransactionResult();
        }

        public async Task<TransactionResult> Remove(TransactionContext context, FunctionInput functionInput)
        {
            var accountID = functionInput.DestinationGrains.First();
            BankAccount myState = await this.GetState(context, AccessMode.ReadWrite);
            this.logger.LogInformation("Going to remove on this account: {accountId}, secondValue = {secondValue}", this.GrainReference, accountID, accountID.SecondValue);
            myState.Balance /= accountID.Value;
            myState.Balance += accountID.SecondValue;

            this.logger.LogInformation("Balance {balance} after remove {accountId}", this.GrainReference, accountID, myState.Balance);

            return new TransactionResult();
        }

        public async Task<TransactionResult> Balance(TransactionContext context, FunctionInput functionInput)
        {
            BankAccount myState = await this.GetState(context, AccessMode.Read);

            return new TransactionResult()
            {
                Result = myState.Balance
            };
        }

        public Task<BankAccount> GetState()
        {
            return base.GetState();
        }
    }
}