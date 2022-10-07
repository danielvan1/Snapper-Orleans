using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionBroadcasting;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Implementation.TransactionExecution.TransactionExecution;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution
{
    [Reentrant]
    [TransactionExecutionGrainPlacementStrategy]
    public abstract class TransactionExecutionGrain<TState> : Grain, ITransactionExecutionGrain where TState : ICloneable, ISerializable, new()
    {
        private readonly ILogger<TransactionExecutionGrain<TState>> logger;
        private readonly ITransactionContextProviderFactory transactionContextProviderFactory;
        private readonly ITransactionBroadCasterFactory transactionBroadCasterFactory;
        private readonly IDeterministicTransactionExecutorFactory deterministicTransactionExecutorFactory;
        private readonly string classNameSpace;
        private ITransactionContextProvider transactionContextProvider;
        private ITransactionBroadCaster transactionBroadCaster;
        private IDeterministicTransactionExecutor deterministicTransactionExecutor;
        private GrainId myGrainId;

        // transaction execution
        private ITransactionalState<TState> state;

        public TransactionExecutionGrain(ILogger<TransactionExecutionGrain<TState>> logger,
                                         ITransactionContextProviderFactory transactionContextProviderFactory,
                                         ITransactionBroadCasterFactory transactionBroadCasterFactory,
                                         IDeterministicTransactionExecutorFactory deterministicTransactionExecutorFactory,
                                         string classNameSpace)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.transactionContextProviderFactory = transactionContextProviderFactory ?? throw new ArgumentNullException(nameof(transactionContextProviderFactory));
            this.transactionBroadCasterFactory = transactionBroadCasterFactory ?? throw new ArgumentNullException(nameof(transactionBroadCasterFactory));
            this.deterministicTransactionExecutorFactory = deterministicTransactionExecutorFactory ?? throw new ArgumentNullException(nameof(deterministicTransactionExecutorFactory));
            this.classNameSpace = classNameSpace ?? throw new ArgumentNullException(nameof(classNameSpace));
        }

        public override Task OnActivateAsync()
        {
            this.myGrainId = new GrainId() { IntId = (int)this.GetPrimaryKeyLong(out string localRegion), StringId = localRegion };
            this.state = new DeterministicState<TState>();

            this.deterministicTransactionExecutor = this.deterministicTransactionExecutorFactory.Create(this.GrainFactory, this.GrainReference, this.myGrainId);
            this.transactionContextProvider = this.transactionContextProviderFactory.Create(this.GrainFactory, this.GrainReference, this.myGrainId);
            this.transactionBroadCaster = this.transactionBroadCasterFactory.Create(this.GrainFactory);

            return Task.CompletedTask;
        }

        /// <summary>
        /// This methods is called by the client. It is the entry point of the transaction.
        /// !!!Notice: the current implementation assumes each actor will be accessed at most once
        /// </summary>
        /// <param name="firstFunction"></param>
        /// <param name="functionInput"></param>
        /// <param name="grainAccessInfo"></param>
        /// <param name="grainClassNames"></param>
        /// <returns></returns>
        public async Task<TransactionResult> StartTransaction(string firstFunction, object functionInput, List<GrainAccessInfo> grainAccessInfo)
        {
            var receiveTxnTime = DateTime.Now;

            this.logger.LogInformation("StartTransaction called with startFunc: {startFunc}, funcInput: {funcInput}, grainAccessInfo: [{grainAccessInfo}]",
                                       this.GrainReference, firstFunction, functionInput, string.Join(", ", grainAccessInfo));

            _ = this.transactionBroadCaster.StartTransactionInAllOtherRegions(firstFunction, functionInput, grainAccessInfo);
            // This is where we get the Tuple<Tid, TransactionContext>
            // The TransactionContext just contains the 4 values (localBid, localTid, globalBid, globalTid)
            // to decide the locality of the transaction
            Tuple<long, TransactionContext> transactionContext = await this.transactionContextProvider.GetDeterministicContext(grainAccessInfo);

            await this.deterministicTransactionExecutor.GarbageCollection(transactionContext.Item1);
            var context = transactionContext.Item2;

            // TODO: Only gets here in multi-server or multi-home transaction???

            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction2", this.GrainReference);
            // execute PACT
            var functionCall = new FunctionCall(firstFunction, functionInput, GetType());
            var result = await this.ExecuteDeterministicTransaction(functionCall, context);
            var finishExeTime = DateTime.Now;
            var startExeTime = result.Item2;
            var resultObj = result.Item1;

            // wait for this batch to commit
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction3", this.GrainReference);
            await this.deterministicTransactionExecutor.WaitForBatchToCommit(context.localBid);
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction4", this.GrainReference);

            var commitTime = DateTime.Now;
            var txnResult = new TransactionResult(resultObj);
            txnResult.prepareTime = (startExeTime - receiveTxnTime).TotalMilliseconds;
            txnResult.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            txnResult.commitTime = (commitTime - finishExeTime).TotalMilliseconds;

            return txnResult;
        }

        public async Task<TransactionResult> StartReplicaTransaction(string firstFunction, object functionInput, List<GrainAccessInfo> grainAccessInfo)
        {
            var receiveTxnTime = DateTime.Now;

            this.logger.LogInformation("StartReplicaTransaction called with startFunc: {startFunc}, funcInput: {funcInput}, grainAccessInfo: [{grainAccessInfo}]",
                                       this.GrainReference, firstFunction, functionInput, string.Join(", ", grainAccessInfo));
            this.logger.LogInformation("HerpDerp: {hj} ", this.GrainReference, this.GrainReference);

            Tuple<long, TransactionContext> transactionContext = await this.transactionContextProvider.GetDeterministicContext(grainAccessInfo);
            await this.deterministicTransactionExecutor.GarbageCollection(transactionContext.Item1);
            var context = transactionContext.Item2;

            // TODO: Only gets here in multi-server or multi-home transaction???

            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction2", this.GrainReference);
            // execute PACT
            var functionCall = new FunctionCall(firstFunction, functionInput, GetType());
            var result = await this.ExecuteDeterministicTransaction(functionCall, context);
            var finishExeTime = DateTime.Now;
            var startExeTime = result.Item2;
            var resultObj = result.Item1;

            // wait for this batch to commit
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction3", this.GrainReference);
            await this.deterministicTransactionExecutor.WaitForBatchToCommit(context.localBid);
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction4", this.GrainReference);

            var commitTime = DateTime.Now;
            var txnResult = new TransactionResult(resultObj);
            txnResult.prepareTime = (startExeTime - receiveTxnTime).TotalMilliseconds;
            txnResult.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            txnResult.commitTime = (commitTime - finishExeTime).TotalMilliseconds;

            return txnResult;
        }


        public async Task<Tuple<object, DateTime>> ExecuteDeterministicTransaction(FunctionCall call, TransactionContext context)
        {
            this.logger.LogInformation("ExecutingDeterministicTransaction: Function input: {functionName} and transaction context {context}",
                                        this.GrainReference, call.funcName, context);

            await this.deterministicTransactionExecutor.WaitForTurn(context);
            var time = DateTime.Now;

            var transactionResult = await this.InvokeFunction(call, context);   // execute the function call;

            await this.deterministicTransactionExecutor.FinishExecuteDeterministicTransaction(context);

            await this.deterministicTransactionExecutor.CleanUp(context.localTid);
            this.logger.LogInformation("Finished executing deterministic transaction with functioncall {call} and context {context} ",
                                        this.GrainReference, call, context);

            return new Tuple<object, DateTime>(transactionResult.resultObj, time);
        }

        /// <summary>
        /// The LocalCoordinator calls this method to sent the subbatch to executed here.
        /// We first register the batch in the TransactionSceduler. Then we notify the
        /// the transactions which are waiting for the batch to arrive that it can start
        /// the next step which is figuring whether it can execute in the TransactionScheduler
        /// </summary>
        /// <param name="batch"></param>
        /// <returns></returns>
        public Task ReceiveBatchSchedule(LocalSubBatch batch)
        {
            this.deterministicTransactionExecutor.ReceiveBatchSchedule(batch);

            return Task.CompletedTask;
        }

        public Task AckBatchCommit(long bid)
        {
            this.deterministicTransactionExecutor.AckBatchCommit(bid);

            return Task.CompletedTask;
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public async Task<TState> GetState(TransactionContext cxt, AccessMode mode)
        {
            return this.deterministicTransactionExecutor.GetState<TState>(cxt.localTid, mode, this.state);
        }

        /// <summary> When execute a transaction, call this interface to make a cross-grain function invocation </summary>
        public async Task<TransactionResult> CallGrain(TransactionContext context, Tuple<int, string> grainId, string grainNameSpace, FunctionCall call)
        {
            var grain = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Item1, grainId.Item2, grainNameSpace);

            this.logger.LogInformation("Inside CallGrain, going to call grain to execute transaction: {grainId}-{region}", this.GrainReference, grainId.Item1, grainId.Item2);

            var resultObj = (await grain.ExecuteDeterministicTransaction(call, context)).Item1;

            this.logger.LogInformation("Inside CallGrain, after call to grain: {graindId}-{region}", this.GrainReference, grainId.Item1, grainId.Item2 );

            return new TransactionResult(resultObj);
        }

        private async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext context)
        {
            if (context.localBid == -1)
            {
                //this.logger.Error(1, $"[{id}-{region}] Inside of this cxt.localBid == -1 ??");
            }

            MethodInfo methodInfo = call.grainClassName.GetMethod(call.funcName);

            this.logger.LogInformation("Going to call Invoke for method {functionName} with input {input} and context: {context}", this.GrainReference, call.funcName, call.funcInput, context);

            var transactionResult = (Task<TransactionResult>)methodInfo.Invoke(this, new object[] { context, call.funcInput });

            var result = await transactionResult;

            this.logger.LogInformation("Finished with invoking function {name} with input: {input}", this.GrainReference, call.funcName, call.funcInput);

            return result;
        }
    }
}
