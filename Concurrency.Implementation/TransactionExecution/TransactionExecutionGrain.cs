using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution.Nondeterministic;
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
        private readonly ILogger logger;

        private TransactionExecutionGrainId myId;
        // grain basic info
        private string mySiloID;
        readonly ICoordMap coordMap;
        readonly string myClassName;
        static int myLocalCoordID;
        static ILocalCoordinatorGrain myLocalCoord;   // use this coord to get tid for local transactions
        static IGlobalCoordinatorGrain myGlobalCoord;

        // transaction execution
        TransactionScheduler myScheduler;
        ITransactionalState<TState> state;

        // PACT execution
        DetTxnExecutor<TState> detTxnExecutor;
        Dictionary<long, TaskCompletionSource<bool>> batchCommit;                // key: local bid

        // ACT execution
        Dictionary<long, int> coordinatorMap;
        NonDetTxnExecutor<TState> nonDetTxnExecutor;
        NonDetCommitter<TState> nonDetCommitter;

        // garbage collection
        long highestCommittedLocalBid;

        private SiloInfo siloInfo;

        public TransactionExecutionGrain(ILogger logger, string myClassName)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.myClassName = myClassName;
        }

        public Task CheckGC()
        {
            state.CheckGC();
            myScheduler.CheckGC();
            detTxnExecutor.CheckGC();
            nonDetTxnExecutor.CheckGC();
            nonDetCommitter.CheckGC();
            if (batchCommit.Count != 0) Console.WriteLine($"TransactionExecutionGrain: batchCommit.Count = {batchCommit.Count}");
            if (coordinatorMap.Count != 0) Console.WriteLine($"TransactionExecutionGrain: coordinatorMap.Count = {coordinatorMap.Count}");

            return Task.CompletedTask;
        }

        public override Task OnActivateAsync()
        {
            highestCommittedLocalBid = -1;

            this.myId = new TransactionExecutionGrainId()
            {
                IntId = (int)this.GetPrimaryKeyLong(out string localRegion),
                StringId = localRegion
            };

            this.mySiloID = localRegion;

            // transaction execution
            // loggerGroup.GetLoggingProtocol(myID, out log);
            this.myScheduler = new TransactionScheduler(this.myId.IntId);
            this.state = new HybridState<TState>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.coordinatorMap = new Dictionary<long, int>();

            myLocalCoord = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(this.myId.IntId % Constants.NumberOfLocalCoordinatorsPerSilo, this.myId.StringId);

            // TODO: Need this later when we have multi server and multi home
            // var globalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
            // myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);

            // TODO: Consider this logic for how regional coordinators are chosen
            var regionalCoordinatorID = 0;
            var regionalCoordinator = GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordinatorID, "EU");

            this.logger.LogInformation("Init DetTxnExecutor", this.GrainReference);
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
                myScheduler,
                state);
            this.logger.LogInformation("After Init of DetTxnExecutor", this.GrainReference);

            return Task.CompletedTask;
        }

        // Notice: the current implementation assumes each actor will be accessed at most once
        // TODO: Change the grainAccessInfo to correspond to the current way we use ids.
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName)
        {
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction1", this.GrainReference);
            var receiveTxnTime = DateTime.Now;

            this.logger.LogInformation("Going to call DetTxnExecutor.GetDetContext", this.GrainReference);
            // This is where we get the Tuple<Tid, TransactionContext>
            // The TransactionContext just contains the 4 values (localBid, localTid, globalBid, globalTid)
            // to decide the locality of the transaction
            Tuple<long, TransactionContext> transactionContext = await this.detTxnExecutor.GetDetContext(grainAccessInfo, grainClassName);
            var cxt = transactionContext.Item2;

            // Only gets here in multi-server or multi-home transaction
            if (highestCommittedLocalBid < transactionContext.Item1)
            {
                highestCommittedLocalBid = transactionContext.Item1;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }

            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction2", this.GrainReference);
            // execute PACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var res = await ExecuteDet(call, cxt);
            var finishExeTime = DateTime.Now;
            var startExeTime = res.Item2;
            var resultObj = res.Item1;

            // wait for this batch to commit
            this.logger.LogInformation("TransactionExecutionGrain: StartTransaction3", this.GrainReference);
            await WaitForBatchCommit(cxt.localBid);
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
            this.logger.LogInformation($"{this.myId.IntId}-{this.myId.StringId} ReceiveBatchSchedule was called with bid: {batch.bid}", this.GrainReference);
            // do garbage collection for committed local batches
            if (highestCommittedLocalBid < batch.highestCommittedBid)
            {
                highestCommittedLocalBid = batch.highestCommittedBid;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }
            batchCommit.Add(batch.bid, new TaskCompletionSource<bool>());

            // register the local SubBatch info

            this.logger.LogInformation($"ReceiveBatchSchedule: registerBatch", this.GrainReference);
            this.myScheduler.RegisterBatch(batch, batch.globalBid, highestCommittedLocalBid);
            this.logger.LogInformation($"ReceiveBatchSchedule: batchArrive. detTxnExecutor: {detTxnExecutor}", this.GrainReference);
            this.detTxnExecutor.BatchArrive(batch);
            this.logger.LogInformation($"ReceiveBatchSchedule: detTxnExecutor.BatchArrive(batch);", this.GrainReference);

            return Task.CompletedTask;
        }

        /// <summary> When commit an ACT, call this interface to wait for a specific local batch to commit </summary>
        public async Task WaitForBatchCommit(long bid)
        {
            if (highestCommittedLocalBid >= bid) return;
            this.logger.LogInformation($"Waiting for batch id:{bid} to commit", this.GrainReference);
            await batchCommit[bid].Task;
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(long bid)
        {
            this.logger.LogInformation($"DetTxnExecutor.AckBatchCommit is called on batch id:{bid} by local coordinator", this.GrainReference);
            if (highestCommittedLocalBid < bid)
            {
                highestCommittedLocalBid = bid;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }
            batchCommit[bid].SetResult(true);
            batchCommit.Remove(bid);
            //myScheduler.AckBatchCommit(highestCommittedBid);
            return Task.CompletedTask;
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
            var isDeterministic = true;
            if (isDeterministic)
            {
                return detTxnExecutor.GetState(cxt.localTid, mode);
            }
            else
            {
                // TODO: Rename to context.regionalTransactionId if
                // we find out we need to run this(Which I think we do)
                return await nonDetTxnExecutor.GetState(cxt.globalTid, mode);
            }
        }

        public async Task<Tuple<object, DateTime>> ExecuteDet(FunctionCall call, TransactionContext cxt)
        {
            this.logger.LogInformation($"{this.myId.IntId}-{this.myId.StringId} TransactionExecutionGrain: detTxnExecutor.WaitForTurn(cxt)", this.GrainReference);
            await this.detTxnExecutor.WaitForTurn(cxt);
            var time = DateTime.Now;
            this.logger.LogInformation($"{this.myId.IntId}-{this.myId.StringId} TransactionExecutionGrain: await InvokeFunction(call, cxt)", this.GrainReference);
            var txnRes = await InvokeFunction(call, cxt);   // execute the function call;
            this.logger.LogInformation($"{this.myId.IntId}-{this.myId.StringId} TransactionExecutionGrain: await detTxnExecutor.FinishExecuteDetTxn(cxt);", this.GrainReference);
            await this.detTxnExecutor.FinishExecuteDetTxn(cxt);
            this.logger.LogInformation($"{this.myId.IntId}-{this.myId.StringId} TransactionExecutionGrain: (after) await detTxnExecutor.FinishExecuteDetTxn(cxt);", this.GrainReference);
            this.detTxnExecutor.CleanUp(cxt.localTid);
            return new Tuple<object, DateTime>(txnRes.resultObj, time);
        }

        public async Task<Tuple<NonDetFuncResult, DateTime>> ExecuteNonDet(FunctionCall call, TransactionContext cxt)
        {
            var canExecute = await nonDetTxnExecutor.WaitForTurn(cxt.globalTid);
            var time = DateTime.Now;
            if (canExecute == false)
            {
                var funcResult = new NonDetFuncResult();
                funcResult.Exp_Deadlock = true;
                funcResult.exception = true;
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                return new Tuple<NonDetFuncResult, DateTime>(funcResult, time);
            }
            else
            {
                var exception = false;
                Object resultObj = null;
                try
                {
                    var txnRes = await InvokeFunction(call, cxt);
                    resultObj = txnRes.resultObj;
                }
                catch (Exception)
                {
                    // exceptions thrown from GetState will be caught here
                    exception = true;
                }
                var funcResult = nonDetTxnExecutor.UpdateExecutionResult(cxt.globalTid, highestCommittedLocalBid);
                if (resultObj != null) funcResult.SetResultObj(resultObj);
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                if (exception) CleanUp(cxt.globalTid);
                return new Tuple<NonDetFuncResult, DateTime>(funcResult, time);
            }
        }

        async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext cxt)
        {
            var id = this.GetPrimaryKeyLong(out string region);
            this.logger.Info($"[{id}-{region}] Inside of InvokeFunction");
            if (cxt.localBid == -1)
            {
                this.logger.Error(1, $"[{id}-{region}] Inside of this cxt.localBid == -1 ??");
                Debug.Assert(!coordinatorMap.ContainsKey(cxt.globalTid));
                coordinatorMap.Add(cxt.globalTid, cxt.nonDetCoordID);
            }
            var mi = call.grainClassName.GetMethod(call.funcName);
            this.logger.Info($"[{id}-{region}] going to call mi.Invoke for method {call.funcName} on {this}, {cxt}, {call.funcInput} ");
            var t = (Task<TransactionResult>)mi.Invoke(this, new object[] { cxt, call.funcInput });
            this.logger.Info($"[{id}-{region}] After call to mi.Invoke on {this}, {cxt} {call.funcInput} ");
            this.logger.Info($"[{id}-{region}] After call to mi.Invoke, waiting for task to complete");
            var result = await t;
            this.logger.Info($"[{id}-{region}] After call to mi.Invoke, AFTER waiting for task to complete");
            return result;
        }

        /// <summary> When execute a transaction, call this interface to make a cross-grain function invocation </summary>
        public Task<TransactionResult> CallGrain(TransactionContext cxt, Tuple<int, string> grainID, string grainNameSpace, FunctionCall call)
        {
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.Item1, grainID.Item2, grainNameSpace);

            // Question: How do we detect when our transactions just create
            // new actors locally because our silo key is not being used as
            // intended?
            // Question: do we need this old check isDet = cxt.localBid != 1
            // if we only run PACTs ?

            //var isDet = cxt.localBid != 1;
            var isDeterministic = true;
            if (isDeterministic)
            {
                return this.detTxnExecutor.CallGrain(cxt, call, grain);
            }
            else
            {
                return this.nonDetTxnExecutor.CallGrain(cxt, call, grain);
            }
        }

        public async Task<bool> Prepare(long tid, bool isReader)
        {
            var vote = await nonDetCommitter.Prepare(tid, isReader);
            if (isReader) CleanUp(tid);
            return vote;
        }

        // only writer grain needs 2nd phase of 2PC
        public async Task Commit(long tid, long maxBeforeLocalBid, long maxBeforeGlobalBid)
        {
            nonDetTxnExecutor.Commit(maxBeforeLocalBid, maxBeforeGlobalBid);
            await nonDetCommitter.Commit(tid);
            CleanUp(tid);
        }

        public Task Abort(long tid)
        {
            nonDetCommitter.Abort(tid);
            CleanUp(tid);
            return Task.CompletedTask;
        }

        void CleanUp(long tid)
        {
            coordinatorMap.Remove(tid);
            myScheduler.scheduleInfo.CompleteNonDetTxn(tid);
        }
    }
}
