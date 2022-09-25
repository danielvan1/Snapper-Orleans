using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
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
        private GrainId myId;

        // grain basic info
        private string mySiloID;
        private readonly string myClassName;
        private static int myLocalCoordID;
        private static ILocalCoordinatorGrain myLocalCoord;   // use this coord to get tid for local transactions

        // transaction execution
        private TransactionScheduler transactionScheduler;
        private ITransactionalState<TState> state;

        // PACT execution
        private DetTxnExecutor<TState> detTxnExecutor;
        private Dictionary<long, TaskCompletionSource<bool>> batchCommit;                // key: local bid


        // garbage collection
        private long highestCommittedLocalBid;

        public TransactionExecutionGrain(ILogger<TransactionExecutionGrain<TState>> logger, string myClassName)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.myClassName = myClassName;
        }

        public override Task OnActivateAsync()
        {
            this.highestCommittedLocalBid = -1;

            this.myId = new GrainId()
            {
                IntId = (int)this.GetPrimaryKeyLong(out string localRegion),
                StringId = localRegion
            };

            this.mySiloID = localRegion;

            // transaction execution
            // loggerGroup.GetLoggingProtocol(myID, out log);
            this.transactionScheduler = new TransactionScheduler();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.state = new DeterministicState<TState>();

            var myLocalCoord = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(this.myId.IntId % Constants.NumberOfLocalCoordinatorsPerSilo, this.myId.StringId);

            // TODO: Need this later when we have multi server and multi home
            // var globalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
            // myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);

            // TODO: Consider this logic for how regional coordinators are chosen
            var regionalCoordinatorID = 0;
            var regionalCoordinator = GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordinatorID, "EU");

            this.logger.LogInformation("Initialized DetTxnExecutor", this.GrainReference);
            this.detTxnExecutor = new DetTxnExecutor<TState>(
                this.logger,
                this.GrainReference,
                this.myId,
                this.myId.IntId,
                mySiloID,
                myLocalCoordID,
                myLocalCoord,
                regionalCoordinator,
                GrainFactory,
                transactionScheduler,
                state);

            return Task.CompletedTask;
        }

        // Notice: the current implementation assumes each actor will be accessed at most once
        // TODO: Change the grainAccessInfo to correspond to the current way we use ids.
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassNames)
        {
            this.logger.LogInformation("StartTransaction called with startFunc: {startFunc}, funcInput: {funcInput}, grainAccessInfo: [{grainAccessInfo}], grainClassNames: [{grainClassNames}] ",
                                       this.GrainReference, startFunc, funcInput, string.Join(", ", grainAccessInfo), string.Join(", ", grainClassNames));


            var receiveTxnTime = DateTime.Now;

            // This is where we get the Tuple<Tid, TransactionContext>
            // The TransactionContext just contains the 4 values (localBid, localTid, globalBid, globalTid)
            // to decide the locality of the transaction
            Tuple<long, TransactionContext> transactionContext = await this.detTxnExecutor.GetDetContext(grainAccessInfo, grainClassNames);
            var context = transactionContext.Item2;

            // Only gets here in multi-server or multi-home transaction
            if (this.highestCommittedLocalBid < transactionContext.Item1)
            {
                this.highestCommittedLocalBid = transactionContext.Item1;
                this.transactionScheduler.AckBatchCommit(highestCommittedLocalBid);
            }

            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction2", this.GrainReference);
            // execute PACT
            var functionCall = new FunctionCall(startFunc, funcInput, GetType());
            var result = await this.ExecuteDeterministicTransaction(functionCall, context);
            var finishExeTime = DateTime.Now;
            var startExeTime = result.Item2;
            var resultObj = result.Item1;

            // wait for this batch to commit
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction3", this.GrainReference);
            await WaitForBatchCommit(context.localBid);
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction4", this.GrainReference);

            var commitTime = DateTime.Now;
            var txnResult = new TransactionResult(resultObj);
            txnResult.prepareTime = (startExeTime - receiveTxnTime).TotalMilliseconds;
            txnResult.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            txnResult.commitTime = (commitTime - finishExeTime).TotalMilliseconds;
            return txnResult;
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public Task ReceiveBatchSchedule(LocalSubBatch batch)
        {
            this.logger.LogInformation("ReceiveBatchSchedule was called with bid: {batch} from coordinator {intId}",
                                       this.GrainReference, batch.Bid, batch.CoordinatorId);

            // do garbage collection for committed local batches
            if (this.highestCommittedLocalBid < batch.HighestCommittedBid)
            {
                this.highestCommittedLocalBid = batch.HighestCommittedBid;
                this.transactionScheduler.AckBatchCommit(this.highestCommittedLocalBid);
            }

            this.batchCommit.Add(batch.Bid, new TaskCompletionSource<bool>());

            // register the local SubBatch info

            this.logger.LogInformation($"ReceiveBatchSchedule: registerBatch", this.GrainReference);
            this.transactionScheduler.RegisterBatch(batch, batch.GlobalBid, highestCommittedLocalBid);
            this.logger.LogInformation($"ReceiveBatchSchedule: batchArrive. detTxnExecutor: {detTxnExecutor}", this.GrainReference);
            this.detTxnExecutor.BatchArrive(batch);
            this.logger.LogInformation($"ReceiveBatchSchedule: detTxnExecutor.BatchArrive(batch);", this.GrainReference);

            return Task.CompletedTask;
        }

        /// <summary> When commit an ACT, call this interface to wait for a specific local batch to commit </summary>
        public async Task WaitForBatchCommit(long bid)
        {
            if (this.highestCommittedLocalBid >= bid) return;
            this.logger.LogInformation("Waiting for batch id: {bid} to commit", this.GrainReference, bid);
            await batchCommit[bid].Task;
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(long bid)
        {
            this.logger.LogInformation("DetTxnExecutor.AckBatchCommit is called on batch id: {bid} by local coordinator", this.GrainReference, bid);
            if (this.highestCommittedLocalBid < bid)
            {
                this.highestCommittedLocalBid = bid;
                this.transactionScheduler.AckBatchCommit(highestCommittedLocalBid);
            }

            this.batchCommit[bid].SetResult(true);
            this.batchCommit.Remove(bid);
            //myScheduler.AckBatchCommit(highestCommittedBid);
            return Task.CompletedTask;
        }

        public async Task<Tuple<object, DateTime>> ExecuteDeterministicTransaction(FunctionCall call, TransactionContext context)
        {
            this.logger.LogInformation($"detTxnExecutor.WaitForTurn(cxt)", this.GrainReference);
            await this.detTxnExecutor.WaitForTurn(context);
            var time = DateTime.Now;

            this.logger.LogInformation($"InvokeFunction(call, cxt)", this.GrainReference);
            var transactionResult = await InvokeFunction(call, context);   // execute the function call;
            this.logger.LogInformation($"DetTxnExecutor.FinishExecuteDetTxn(cxt);", this.GrainReference);

            await this.detTxnExecutor.FinishExecuteDeterministicTransaction(context);

            this.logger.LogInformation($"(after) DetTxnExecutor.FinishExecuteDetTxn(cxt);", this.GrainReference);
            this.detTxnExecutor.CleanUp(context.localTid);

            return new Tuple<object, DateTime>(transactionResult.resultObj, time);
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public async Task<TState> GetState(TransactionContext cxt, AccessMode mode)
        {
            // TODO: I think that for multi-server we should actually
            // run the nonDetTxnExecutor path of this code, but why is multi-server
            // considered non-det ? Right now I think that the RegionalBankClient
            // works because we always run the deterministic, which will just create
            // the actors it need locally (I think).
            // Question: How do we detect when our transactions just create
            // new actors locally because our silo key is not being used as
            // intended?

            //var isDet = cxt.localBid != -1;
            return this.detTxnExecutor.GetState(cxt.localTid, mode);
        }

        /// <summary> When execute a transaction, call this interface to make a cross-grain function invocation </summary>
        public async Task<TransactionResult> CallGrain(TransactionContext cxt, Tuple<int, string> grainID, string grainNameSpace, FunctionCall call)
        {
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.Item1, grainID.Item2, grainNameSpace);

            // Question: How do we detect when our transactions just create
            // new actors locally because our silo key is not being used as
            // intended?
            // Question: do we need this old check isDet = cxt.localBid != 1
            // if we only run PACTs ?

            this.logger.LogInformation("Inside CallGrain, going to call (await grain.ExecuteDet(call, cxt))", this.GrainReference);
            var resultObj = (await grain.ExecuteDeterministicTransaction(call, cxt)).Item1;
            this.logger.LogInformation("Inside CallGrain, after call to (await grain.ExecuteDet(call, cxt))", this.GrainReference);

            return new TransactionResult(resultObj);
        }

        private async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext cxt)
        {
            if (cxt.localBid == -1)
            {
                //this.logger.Error(1, $"[{id}-{region}] Inside of this cxt.localBid == -1 ??");
            }

            MethodInfo methodInfo = call.grainClassName.GetMethod(call.funcName);

            this.logger.LogInformation("Going to call Invoke for method {functionName} with input {input}", this.GrainReference, call.funcName, call.funcInput);

            var transactionResult = (Task<TransactionResult>)methodInfo.Invoke(this, new object[] { cxt, call.funcInput });

            var result = await transactionResult;

            return result;
        }

    }
}
