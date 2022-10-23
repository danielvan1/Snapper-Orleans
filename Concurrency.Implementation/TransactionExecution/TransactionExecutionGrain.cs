using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionBroadcasting;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Implementation.TransactionExecution.TransactionExecution;
using Concurrency.Interface.Coordinator;
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
            this.myGrainId = new GrainId() { IntId = (int)this.GetPrimaryKeyLong(out string localRegion), SiloId = localRegion, GrainClassName = this.classNameSpace };
            this.state = new DeterministicState<TState>();
            var myLocalCoord = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(this.myGrainId.IntId % Constants.NumberOfLocalCoordinatorsPerSilo, this.myGrainId.SiloId);

            // TODO: Need this later when we have multi server and multi home
            // var globalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
            // myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);

            // TODO: Consider this logic for how regional coordinators are chosen
            var regionalCoordinatorID = 0;
            var regionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordinatorID, this.myGrainId.SiloId.Substring(0, 2));
            this.deterministicTransactionExecutor = this.deterministicTransactionExecutorFactory.Create(this.GrainFactory, this.GrainReference, this.myGrainId);
            this.transactionContextProvider = this.transactionContextProviderFactory.Create(this.GrainFactory, this.GrainReference, this.myGrainId, myLocalCoord, regionalCoordinator);
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
        public async Task<TransactionResult> StartTransaction(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfo)
        {
            var receiveTxnTime = DateTime.Now;

            this.logger.LogInformation("StartTransaction called with startFunc: {startFunc}, funcInput: {funcInput}, grainAccessInfo: [{grainAccessInfo}]",
                                       this.GrainReference, firstFunction, functionInput, string.Join(", ", grainAccessInfo));

            Tuple<long, TransactionContext> transactionContext = await this.transactionContextProvider.GetDeterministicContext(grainAccessInfo);

            _ = this.transactionBroadCaster.StartTransactionInAllOtherRegions(firstFunction, functionInput, grainAccessInfo, this.myGrainId, transactionContext.Item2, transactionContext.Item1);

            return await this.RunTransaction(firstFunction, functionInput, grainAccessInfo, transactionContext.Item2, transactionContext.Item1);
        }

        public async Task<TransactionResult> StartReplicaTransaction(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfo, TransactionContext transactionContext, long highestCommittedBidFromMaster)
        {
            transactionContext.IsReplicaTransaction = true;

            return await this.RunTransaction(firstFunction, functionInput, grainAccessInfo, transactionContext, highestCommittedBidFromMaster);
        }

        private async Task<TransactionResult> RunTransaction(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfo, TransactionContext transactionContext, long highestCommittedBid)
        {
            var receiveTxnTime = DateTime.Now;

            this.logger.LogInformation("StartReplicaTransaction called with startFunc: {startFunc}, funcInput: {funcInput}, grainAccessInfo: [{grainAccessInfo}]",
                                       this.GrainReference, firstFunction, functionInput, string.Join(", ", grainAccessInfo));

            await this.deterministicTransactionExecutor.GarbageCollection(highestCommittedBid);

            // TODO: Only gets here in multi-server or multi-home transaction???

            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction2", this.GrainReference);
            // execute PACT
            var functionCall = new FunctionCall(firstFunction, functionInput, GetType());

            var result = await this.ExecuteDeterministicTransaction(functionCall, transactionContext);
            var finishExeTime = DateTime.Now;
            var startExeTime = result.Item2;
            var resultObj = result.Item1;

            // wait for this batch to commit
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction3", this.GrainReference);
            await this.deterministicTransactionExecutor.WaitForBatchToCommit(transactionContext.localBid);
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

        public async Task<TState> GetState()
        {
            return this.state.DetOp();
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

            this.logger.LogInformation("Going to call Invoke for method {functionName} and context: {context}", this.GrainReference, call.funcName, context);

            var transactionResult = (Task<TransactionResult>)methodInfo.Invoke(this, new object[] { context, call.funcInput });

            var result = await transactionResult;

            this.logger.LogInformation("Finished with invoking function {name}", this.GrainReference, call.funcName);

            return result;
        }
    }
}