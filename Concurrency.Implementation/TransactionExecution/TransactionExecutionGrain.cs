﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution.Scheduler;
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

        // This is the only Dictionary where we actually wait for a commit. This is used after
        // we get the acknowledgement from the RegionalCoordinator.
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

            this.myId = new GrainId() { IntId = (int)this.GetPrimaryKeyLong(out string localRegion), StringId = localRegion };

            this.mySiloID = localRegion;

            // transaction execution
            // loggerGroup.GetLoggingProtocol(myID, out log);
            this.transactionScheduler = new TransactionScheduler(this.logger);
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.state = new DeterministicState<TState>();

            var myLocalCoord = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(this.myId.IntId % Constants.NumberOfLocalCoordinatorsPerSilo, this.myId.StringId);

            // TODO: Need this later when we have multi server and multi home
            // var globalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
            // myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);

            // TODO: Consider this logic for how regional coordinators are chosen
            var regionalCoordinatorID = 0;
            var regionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordinatorID, "EU");


            this.detTxnExecutor = new DetTxnExecutor<TState>(
                this.logger,
                this.GrainReference,
                this.myId,
                this.myId.IntId,
                this.mySiloID,
                myLocalCoordID,
                myLocalCoord,
                regionalCoordinator,
                GrainFactory,
                transactionScheduler,
                state);

            this.logger.LogInformation("Initialized DetTxnExecutor", this.GrainReference);

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
        public async Task<TransactionResult> StartTransaction(string firstFunction, object functionInput,
                                                              List<Tuple<int, string>> grainAccessInfo, List<string> grainClassNames)
        {
            this.logger.LogInformation("StartTransaction called with startFunc: {startFunc}, funcInput: {funcInput}, grainAccessInfo: [{grainAccessInfo}], grainClassNames: [{grainClassNames}] ",
                                       this.GrainReference, firstFunction, functionInput, string.Join(", ", grainAccessInfo), string.Join(", ", grainClassNames));


            var receiveTxnTime = DateTime.Now;

            // This is where we get the Tuple<Tid, TransactionContext>
            // The TransactionContext just contains the 4 values (localBid, localTid, globalBid, globalTid)
            // to decide the locality of the transaction
            Tuple<long, TransactionContext> transactionContext = await this.detTxnExecutor.GetDetContext(grainAccessInfo, grainClassNames);
            var context = transactionContext.Item2;

            // TODO: Only gets here in multi-server or multi-home transaction???
            if (this.highestCommittedLocalBid < transactionContext.Item1)
            {
                this.highestCommittedLocalBid = transactionContext.Item1;
                this.transactionScheduler.GarbageCollection(this.highestCommittedLocalBid);
            }

            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction2", this.GrainReference);
            // execute PACT
            var functionCall = new FunctionCall(firstFunction, functionInput, GetType());
            var result = await this.ExecuteDeterministicTransaction(functionCall, context);
            var finishExeTime = DateTime.Now;
            var startExeTime = result.Item2;
            var resultObj = result.Item1;

            // wait for this batch to commit
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction3", this.GrainReference);
            await this.WaitForBatchCommit(context.localBid);
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

            await this.detTxnExecutor.WaitForTurn(context);
            var time = DateTime.Now;

            var transactionResult = await this.InvokeFunction(call, context);   // execute the function call;

            await this.detTxnExecutor.FinishExecuteDeterministicTransaction(context);

            this.detTxnExecutor.CleanUp(context.localTid);
            this.logger.LogInformation("Finished executing deterministic transaction with functioncall {call} and context {context} ",
                                        this.GrainReference, call, context);

            return new Tuple<object, DateTime>(transactionResult.resultObj, time);
        }

        /// <summary> When commit an ACT, call this interface to wait for a specific local batch to commit </summary>
        private async Task WaitForBatchCommit(long bid)
        {
            // TODO: When can this happen???
            if (this.highestCommittedLocalBid >= bid) return;

            this.logger.LogInformation("Waiting for bid: {bid} to commit", this.GrainReference, bid);

            await this.batchCommit[bid].Task;

            this.logger.LogInformation("Done waiting for bid: {bid} to commit", this.GrainReference, bid);
        }

        #region Communication with LocalCoordinator

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
            this.logger.LogInformation("Received LocalSubBatch: {batch}",
                                       this.GrainReference, batch);

            // do garbage collection for committed local batches
            if (this.highestCommittedLocalBid < batch.HighestCommittedBid)
            {
                this.highestCommittedLocalBid = batch.HighestCommittedBid;
                this.transactionScheduler.GarbageCollection(this.highestCommittedLocalBid);
            }

            this.batchCommit.Add(batch.Bid, new TaskCompletionSource<bool>());

            // register the local SubBatch info

            this.transactionScheduler.RegisterBatch(batch, batch.RegionalBid, this.highestCommittedLocalBid);
            this.detTxnExecutor.BatchArrive(batch);

            return Task.CompletedTask;
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(long bid)
        {
            this.logger.LogInformation("AckBatchCommit is called on batch id: {bid} by LocalCoordinator",
                                       this.GrainReference, bid);

            if (this.highestCommittedLocalBid < bid)
            {
                this.highestCommittedLocalBid = bid;
                this.transactionScheduler.GarbageCollection(this.highestCommittedLocalBid);
            }

            this.logger.LogInformation("Setting bid: {bid} to commit, these are the bid waiting: {bids}",
                                       this.GrainReference, bid, string.Join(", ", this.batchCommit.Select(kv => kv.Key + " : " + kv.Value.ToString())));

            // Sets this to true, so the await in WaitForBatchCommit will continue.
            this.batchCommit[bid].SetResult(true);

            this.batchCommit.Remove(bid);
            //myScheduler.AckBatchCommit(highestCommittedBid);
            return Task.CompletedTask;
        }

        #endregion



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
        public async Task<TransactionResult> CallGrain(TransactionContext context, Tuple<int, string> grainId, string grainNameSpace, FunctionCall call)
        {
            var grain = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Item1, grainId.Item2, grainNameSpace);

            this.logger.LogInformation("Inside CallGrain {i} --- {ii}", 1, this.GrainReference);
            this.logger.LogInformation("Inside CallGrain, grainId: {id}-{region}", this.GrainReference, grainId.Item1, grainId.Item2);
            this.logger.LogInformation("Inside CallGrain, going to call grain to execute transaction: {grainId}-{region}", this.GrainReference, grainId.Item1, grainId.Item2);
            this.logger.LogInformation("Inside CallGrain {i} --- {ii}", this.GrainReference, 2);

            var resultObj = (await grain.ExecuteDeterministicTransaction(call, context)).Item1;
            this.logger.LogInformation("Inside CallGrain, after call to grain: {graindId}-{region}", this.GrainReference);

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
