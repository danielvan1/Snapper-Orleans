using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution.Scheduler;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DetTxnExecutor<TState> where TState : ICloneable, ISerializable
    {
        private readonly ILogger logger;
        private readonly GrainReference grainReference;
        private readonly GrainId myId;

        // grain basic info
        private readonly string siloID;

        // transaction execution
        private readonly TransactionScheduler myScheduler;
        private readonly ITransactionalState<TState> state;

        // local and global coordinators
        private readonly ILocalCoordinatorGrain myLocalCoord;
        private readonly IRegionalCoordinatorGrain regionalCoordinator;                                // use this coord to get tid for global transactions
        private readonly IGrainFactory grainFactory;

        // PACT execution
        private Dictionary<long, TaskCompletionSource<bool>> localBatchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        private Dictionary<long, TaskCompletionSource<bool>> regionalBatchInfoPromise;      // key: global bid, use to check if the SubBatch has arrived or not

        private Dictionary<long, BasicFuncResult> determinsticFunctionResults;       // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times

        // only for regional PACT
        private Dictionary<long, long> regionalBidToLocalBid;
        private Dictionary<long, Dictionary<long, long>> regionalBidToRegionalTidToLocalTidPerBatch;    // key: global bid, <global tid, local tid>

        public DetTxnExecutor(
            ILogger logger,
            GrainReference grainReference,
            GrainId myId,
            string siloID,
            ILocalCoordinatorGrain myLocalCoord,
            IRegionalCoordinatorGrain myRegionalCoordinator,
            IGrainFactory grainFactory,
            TransactionScheduler myScheduler,
            ITransactionalState<TState> state
            )
        {
            this.logger = logger;
            this.grainReference = grainReference;
            this.myId = myId;

            this.siloID = siloID;
            this.myLocalCoord = myLocalCoord;
            this.regionalCoordinator = myRegionalCoordinator;
            this.grainFactory = grainFactory;
            this.myScheduler = myScheduler;
            this.state = state;

            this.localBatchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
            this.determinsticFunctionResults = new Dictionary<long, BasicFuncResult>();
            this.regionalBidToLocalBid = new Dictionary<long, long>();
            this.regionalBidToRegionalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            this.regionalBatchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // int: the highestCommittedBid get from local coordinator
        /// <summary>
        /// This returns the Bid and TransactionContext. Also start the process of starting to create the (sub)batches
        /// in the RegionalCoordinator (if it is a regional transaction) and also in the LocalCoordinator.
        /// We also figure out whether the transaction is a multi-server or single server transacation.
        /// </summary>
        /// <param name="grainAccessInfos"></param>
        /// <param name="grainClassNames"></param>
        /// <returns></returns>

        // TODO: Figure out whether or not the current transaction .
        public async Task<Tuple<long, TransactionContext>> GetDeterministicContext(List<GrainAccessInfo> grainAccessInfos)
        {
            this.logger.LogInformation("Getting context for grainList: [{grainList}] and grainClassNames: [{grainClassNames}]",
                                       this.grainReference, string.Join(", ", grainAccessInfos));

            // check if the transaction will access multiple silos
            var silos = new List<string>();
            var grainListPerSilo = new Dictionary<string, List<GrainAccessInfo>>();

            // This is the placement manager(PM) code described in the paper
            for (int i = 0; i < grainAccessInfos.Count; i++)
            {
                var siloId = grainAccessInfos[i].Region;

                if (!grainListPerSilo.ContainsKey(siloId))
                {
                    silos.Add(siloId);
                    grainListPerSilo.Add(siloId, new List<GrainAccessInfo>());
                }

                grainListPerSilo[siloId].Add(grainAccessInfos[i]);
            }

            // For a simple example, make sure that only 1 silo is involved in the transaction
            this.logger.LogInformation("Silolist count: {siloListCount}", this.grainReference, silos.Count);
            if (silos.Count > 1)
            {
                // get regional tid from regional coordinator
                // Note the Dictionary<string, Tuple<int, string>> part of the
                // return type of NewTransaction(..) is a map between the region
                // and which local coordinators
                Tuple<TransactionRegisterInfo, Dictionary<string, Tuple<int, string>>> regionalInfo =
                    await this.regionalCoordinator.NewRegionalTransaction(silos);

                var regionalTid = regionalInfo.Item1.Tid;
                var regionalBid = regionalInfo.Item1.Bid;
                Dictionary<string, Tuple<int, string>> siloIDToLocalCoordID = regionalInfo.Item2;

                // send corresponding grainAccessInfo to local coordinators in different silos
                Debug.Assert(grainListPerSilo.ContainsKey(siloID));
                Task<TransactionRegisterInfo> task = null;

                for (int i = 0; i < silos.Count; i++)
                {
                    var siloId = silos[i];
                    Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloId));

                    // TODO: Need a map from coordinator to local coordinator
                    var coordId = siloIDToLocalCoordID[siloId];
                    var localCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordId.Item1, coordId.Item2);

                    // get local tid, bid from local coordinator
                    if (coordId.Item2 == this.siloID)
                    {
                        this.logger.LogInformation($"Is calling NewRegionalTransaction w/ task", this.grainReference);
                        task = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainListPerSilo[siloId]);
                    }
                    else
                    {
                        this.logger.LogInformation($"Is calling NewRegionalTransaction w/o task", this.grainReference);

                        _ = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainListPerSilo[siloId]);
                    }
                }

                Debug.Assert(task != null);
                this.logger.LogInformation($"Waiting for task in GetDetContext", this.grainReference);
                TransactionRegisterInfo localInfo = await task;
                this.logger.LogInformation($"Is DONE waiting for task in GetDetContext, going to return tx context", this.grainReference);
                var regionalContext = new TransactionContext(localInfo.Bid, localInfo.Tid, regionalBid, regionalTid);

                // TODO: What is this -1??
                return new Tuple<long, TransactionContext>(-1, regionalContext) ;
            }

            TransactionRegisterInfo info = await myLocalCoord.NewLocalTransaction(grainAccessInfos);
            this.logger.LogInformation("Received TransactionRegisterInfo {info} from localCoordinator: {coordinator}", this.grainReference, info, this.myLocalCoord);

            var cxt2 = new TransactionContext(info.Tid, info.Bid);
            var localContext = new Tuple<long, TransactionContext>(info.HighestCommittedBid, cxt2);

            return localContext;
        }

        /// <summary>
        ///  Method used from TransactionExecutionGrain to wait for the batch to arrive.
        ///  Then after the batch arrives then we will wait until it is the turn of the current
        ///  TransactionContext to execute. This is based on bid and tid.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task WaitForTurn(TransactionContext context)
        {
            this.logger.LogInformation("Waiting for turn for {context}", this.grainReference, context);
            // check if it is a global PACT
            if (context.regionalBid != -1)
            {
                // wait until the SubBatch has arrived this grain
                if (!this.regionalBatchInfoPromise.ContainsKey(context.regionalBid))
                {
                    this.regionalBatchInfoPromise.Add(context.regionalBid, new TaskCompletionSource<bool>());
                }

                // First wait for the batch to arrive and that we can start executing the transactions in this specific batch.
                await this.regionalBatchInfoPromise[context.regionalBid].Task;

                // need to map global info to the corresponding local tid and bid
                context.localBid = this.regionalBidToLocalBid[context.regionalBid];
                context.localTid = this.regionalBidToRegionalTidToLocalTidPerBatch[context.regionalBid][context.regionalTid];
            }
            else
            {
                this.logger.LogInformation("WaitForturn waiting", this.grainReference);
                // wait until the SubBatch has arrived this grain
                if (!this.localBatchInfoPromise.ContainsKey(context.localBid))
                {
                    this.localBatchInfoPromise.Add(context.localBid, new TaskCompletionSource<bool>());
                }

                // First wait for the batch to arrive and that we can start executing the transactions in this specific batch.
                await this.localBatchInfoPromise[context.localBid].Task;

                this.logger.LogInformation("WaitForturn finished", this.grainReference);
            }

            Debug.Assert(!this.determinsticFunctionResults.ContainsKey(context.localTid));
            this.determinsticFunctionResults.Add(context.localTid, new BasicFuncResult());

            // After the batch is arrived we are waiting for the turn of the current transaction.
            await this.myScheduler.WaitForTurn(context.localBid, context.localTid);
        }

        /// <summary>
        /// Decides whether the batch is complete. If the batch is not complete then we will signal that the next transaction can
        /// execute in the batch. This done by signaling a TaskCompletetionSource in the <see cref="TransactionScheduler"/>.
        /// If the batch is finished then we notify the coordinator that sent the batch that we are done with the current subbatch.
        /// Then, we will return the LocalCoordinatorId. Notice, that the LocalCoordinator,
        /// does not have to be the one that we assigned when initializing the TransactionExecutionGrain. Also the LocalCoordinator
        /// int id is sufficient, since we assume that we always communicate with a LocalCoordinator in the same silo. Hence, the
        /// string (region Id) is equivalent for the LocalCoordinator and TransactionExecutionGrain.
        /// </summary>
        /// <param name="transactionContext"></param>
        /// <returns></returns>
        public async Task FinishExecuteDeterministicTransaction(TransactionContext transactionContext)
        {
            // This is the coordinatorId that sent the subbatch containing the current transaction.
            var localCoordinatorId = this.myScheduler.IsBatchComplete(transactionContext.localBid, transactionContext.localTid);

            this.logger.LogInformation("FinishExecuteDeterministicTransaction: the coordinator that sent the subbatch: {coordinatorId}",
                                        this.grainReference, localCoordinatorId);

            if (localCoordinatorId != -1)   // the current batch has completed on this grain
            {
                this.localBatchInfoPromise.Remove(transactionContext.localBid);

                if (transactionContext.regionalBid != -1)
                {
                    this.regionalBidToLocalBid.Remove(transactionContext.regionalBid);
                    this.regionalBidToRegionalTidToLocalTidPerBatch.Remove(transactionContext.regionalBid);
                    this.regionalBatchInfoPromise.Remove(transactionContext.regionalBid);
                }

                this.myScheduler.CompleteDeterministicBatch(transactionContext.localBid);

                // We use the current region, since we assume that the local coordinator is in the same silo
                var localCoordinatorRegion = this.myId.StringId;

                this.logger.LogInformation("Send the local coordinator {localCoordinatorId}-{localCoordinatorRegion} the acknowledgement of the batch completion for batch id: {localBid}",
                                            this.grainReference, localCoordinatorId, localCoordinatorRegion, transactionContext.localBid);

                // TODO: This coordinator should be the one that sent the batch
                var coordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorId, localCoordinatorRegion);

                _ = coordinator.AckBatchCompletion(transactionContext.localBid);
            }
        }

        /// <summary>
        /// This is called when we receive the batch from the <see cref="LocalCoordinatorGrain"/>.
        /// Signal the batchinfo promise that the batch arrived, so the transaction corresponding to this subbatch
        /// can continue the transaction to the next step of waiting again.
        /// </summary>
        /// <param name="batch"></param>
        public void BatchArrive(LocalSubBatch batch)
        {
            this.logger.LogInformation("Batch arrived, batch: {batch}", this.grainReference, batch);

            // If the batch arrives before the transactions actually reaches the point of localBatchInfoPromise.
            if (!this.localBatchInfoPromise.ContainsKey(batch.Bid))
            {
                this.localBatchInfoPromise.Add(batch.Bid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("BatchArrive: localBtchInfoPromise: {localBatchInfoPromise}",
                                       this.grainReference, this.localBatchInfoPromise[batch.Bid]);

            this.localBatchInfoPromise[batch.Bid].SetResult(true);

            // register global info mapping if necessary
            if (batch.RegionalBid != -1)
            {
                // Mapping the regional bid to the to the local bid
                this.regionalBidToLocalBid.Add(batch.RegionalBid, batch.Bid);
                this.regionalBidToRegionalTidToLocalTidPerBatch.Add(batch.RegionalBid, batch.RegionalTidToLocalTid);

                if (!this.regionalBatchInfoPromise.ContainsKey(batch.RegionalBid))
                {
                    this.regionalBatchInfoPromise.Add(batch.RegionalBid, new TaskCompletionSource<bool>());
                }

                this.regionalBatchInfoPromise[batch.RegionalBid].SetResult(true);
            }
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public TState GetState(long tid, AccessMode mode)
        {
            if (mode == AccessMode.Read)
            {
                this.determinsticFunctionResults[tid].isNoOpOnGrain = false;
                this.determinsticFunctionResults[tid].isReadOnlyOnGrain = true;
            }
            else
            {
                this.determinsticFunctionResults[tid].isNoOpOnGrain = false;
                this.determinsticFunctionResults[tid].isReadOnlyOnGrain = false;
            }

            return state.DetOp();
        }

        public void CleanUp(long tid)
        {
            this.determinsticFunctionResults.Remove(tid);
        }
    }
}
