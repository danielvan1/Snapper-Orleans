using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.LoadBalancing;
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
    public class DeterministicTransactionExecutor : IDeterministicTransactionExecutor
    {
        private readonly ILogger<DeterministicTransactionExecutor> logger;
        private readonly ITransactionScheduler transactionScheduler;

        private readonly ILocalCoordinatorGrain localCoordinator;
        private readonly IRegionalCoordinatorGrain regionalCoordinator;                                // use this coord to get tid for global transactions
        private readonly IGrainFactory grainFactory;
        private readonly ICoordinatorProvider coordinatorProvider;
        private readonly GrainReference grainReference;
        private readonly GrainId grainId;

        // grain basic info
        private readonly string siloId;

        private Dictionary<long, TaskCompletionSource<bool>> batchCommit;                // key: local bid
        private Dictionary<long, TaskCompletionSource<bool>> localBatchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        private Dictionary<long, BasicFuncResult> determinsticFunctionResults;       // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times

        private Dictionary<long, TaskCompletionSource<bool>> regionalBatchInfoPromise;      // key: global bid, use to check if the SubBatch has arrived or not
        private Dictionary<long, long> regionalBidToLocalBid;
        private Dictionary<long, Dictionary<long, long>> regionalBidToRegionalTidToLocalTidPerBatch;    // key: global bid, <global tid, local tid>

        // only for regional PACT

        private long highestCommittedLocalBid;

        public DeterministicTransactionExecutor(ILogger<DeterministicTransactionExecutor> logger,
                                                ICoordinatorProvider coordinatorProvider,
                                                IGrainFactory grainFactory,
                                                GrainReference grainReference,
                                                GrainId grainId)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.coordinatorProvider = coordinatorProvider ?? throw new ArgumentNullException(nameof(coordinatorProvider));
            this.transactionScheduler = new TransactionScheduler();

            this.localCoordinator = coordinatorProvider.GetLocalCoordinatorGrain(grainId.IntId, grainId.StringId, grainFactory);
            this.regionalCoordinator = coordinatorProvider.GetRegionalCoordinator(grainId.IntId, grainId.StringId, grainFactory);


            this.grainReference = grainReference;
            this.grainId = grainId;
            this.siloId = grainId.StringId;

            // this.myLocalCoord = grainFactory;
            // this.regionalCoordinator = regionalCoordinator;
            this.grainFactory = grainFactory;


            this.highestCommittedLocalBid = -1;
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
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
                this.logger.LogInformation("SiloId: {siloId}", this.grainReference, siloId);

                if (!grainListPerSilo.ContainsKey(siloId))
                {
                    silos.Add(siloId);
                    grainListPerSilo.Add(siloId, new List<GrainAccessInfo>());
                }

                GrainAccessInfo grainAccessInfo = new GrainAccessInfo()
                {
                    Id = grainAccessInfos[i].Id,
                    Region = grainAccessInfos[i].Region,
                    GrainClassName = grainAccessInfos[i].GrainClassName,
                };

                grainListPerSilo[siloId].Add(grainAccessInfo);
                // grainAccessInfos[i]
                // grainListPerSilo[siloId].Add();
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
                Debug.Assert(grainListPerSilo.ContainsKey(siloId));
                Task<TransactionRegisterInfo> task = null;

                for (int i = 0; i < silos.Count; i++)
                {
                    var siloId = silos[i];
                    Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloId));

                    // TODO: Need a map from coordinator to local coordinator
                    var coordId = siloIDToLocalCoordID[siloId];
                    var localCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordId.Item1, coordId.Item2);

                    // get local tid, bid from local coordinator
                    this.logger.LogInformation("Chosen LocalCoordinator: {id}-{regionl}, siloId: {siloId}", this.grainReference, coordId.Item1, coordId.Item2, this.siloId);
                    Console.WriteLine($"{this.grainReference}: Chosen LocalCoordinator: {coordId.Item1}-{coordId.Item2}, siloId: {this.siloId}");
                    this.logger.LogInformation("Herp: {x}, {grainReference}", 1, this.grainReference);
                    if (coordId.Item2 == this.siloId)
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

                if (this.highestCommittedLocalBid < -1)
                {
                    this.highestCommittedLocalBid = -1;
                    await this.GarbageCollection(this.highestCommittedLocalBid);
                }
                // TODO: What is this -1??
                return new Tuple<long, TransactionContext>(-1, regionalContext);
            }

            var herpderp = new List<GrainAccessInfo>();

            for (int i = 0; i < grainAccessInfos.Count; i++)
            {
                herpderp.Add(new GrainAccessInfo()
                {
                    Id = grainAccessInfos[i].Id,
                    Region = grainAccessInfos[i].Region,
                    GrainClassName = grainAccessInfos[i].GrainClassName
                }
                );
            }

            TransactionRegisterInfo info = await localCoordinator.NewLocalTransaction(herpderp);
            this.logger.LogInformation("Received TransactionRegisterInfo {info} from localCoordinator: {coordinator}", this.grainReference, info, this.localCoordinator);

            var cxt2 = new TransactionContext(info.Tid, info.Bid);
            var localContext = new Tuple<long, TransactionContext>(info.HighestCommittedBid, cxt2);

            if (this.highestCommittedLocalBid < localContext.Item1)
            {
                this.highestCommittedLocalBid = localContext.Item1;
                await this.GarbageCollection(this.highestCommittedLocalBid);
            }

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

                this.logger.LogInformation("Waiting for regionalbatchInfoPromise for context: {context}", this.grainReference, context);
                // First wait for the batch to arrive and that we can start executing the transactions in this specific batch.
                await this.regionalBatchInfoPromise[context.regionalBid].Task;

                this.logger.LogInformation("Done waiting for regionalbatchInfoPromise for context: {context}", this.grainReference, context);

                // need to map global info to the corresponding local tid and bid
                context.localBid = this.regionalBidToLocalBid[context.regionalBid];
                context.localTid = this.regionalBidToRegionalTidToLocalTidPerBatch[context.regionalBid][context.regionalTid];

                this.logger.LogInformation("HerpDerpContext: {context}", this.grainReference, context);
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
            await this.transactionScheduler.WaitForTurn(context.localBid, context.localTid);
            this.logger.LogInformation("Done waiting for turn for context: {context}", this.grainReference, context);
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
        /// <param name="context"></param>
        /// <returns></returns>
        public Task FinishExecuteDeterministicTransaction(TransactionContext context)
        {
            // This is the coordinatorId that sent the subbatch containing the current transaction.
            var localCoordinatorId = this.transactionScheduler.IsBatchComplete(context.localBid, context.localTid);

            this.logger.LogInformation("FinishExecuteDeterministicTransaction: the coordinator that sent the subbatch: {coordinatorId} with context {context}",
                                        this.grainReference, localCoordinatorId, context);

            if (localCoordinatorId != -1)   // the current batch has completed on this grain
            {
                this.localBatchInfoPromise.Remove(context.localBid);

                if (context.regionalBid != -1)
                {
                    this.regionalBidToLocalBid.Remove(context.regionalBid);
                    this.regionalBidToRegionalTidToLocalTidPerBatch.Remove(context.regionalBid);
                    this.regionalBatchInfoPromise.Remove(context.regionalBid);
                }

                this.transactionScheduler.CompleteDeterministicBatch(context.localBid);

                // We use the current region, since we assume that the local coordinator is in the same silo
                var localCoordinatorRegion = this.grainId.StringId;

                this.logger.LogInformation("Send the local coordinator {localCoordinatorId}-{localCoordinatorRegion} the acknowledgement of the batch completion for batch id: {localBid}",
                                            this.grainReference, localCoordinatorId, localCoordinatorRegion, context.localBid);

                // TODO: This coordinator should be the one that sent the batch
                var coordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorId, localCoordinatorRegion);

                _ = coordinator.BatchCompletionAcknowledgement(context.localBid);

                this.logger.LogInformation("Done with current batch. The coordinator that sent the subbatch: {coordinatorId} with context {context}",
                                           this.grainReference, localCoordinatorId, context);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// This is called when we receive the batch from the <see cref="LocalCoordinatorGrain"/>.
        /// Signal the batchinfo promise that the batch arrived, so the transaction corresponding to this subbatch
        /// can continue the transaction to the next step of waiting again.
        /// </summary>
        /// <param name="batch"></param>
        public Task ReceiveBatchSchedule(LocalSubBatch batch)
        {
            this.logger.LogInformation("Received LocalSubBatch: {batch}",
                                       this.grainReference, batch);

            // do garbage collection for committed local batches
            if (this.highestCommittedLocalBid < batch.HighestCommittedBid)
            {
                this.highestCommittedLocalBid = batch.HighestCommittedBid;
                this.transactionScheduler.GarbageCollection(this.highestCommittedLocalBid);
            }

            this.batchCommit.Add(batch.Bid, new TaskCompletionSource<bool>());

            this.transactionScheduler.RegisterBatch(batch, batch.RegionalBid, this.highestCommittedLocalBid);
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

            return Task.CompletedTask;
        }

        public async Task WaitForBatchToCommit(long bid)
        {
            // TODO: When can this happen???
            if (this.highestCommittedLocalBid >= bid) return;

            this.logger.LogInformation("Waiting for bid: {bid} to commit", this.grainReference, bid);

            await this.batchCommit[bid].Task;

            this.logger.LogInformation("Done waiting for bid: {bid} to commit", this.grainReference, bid);
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(long bid)
        {
            this.logger.LogInformation("AckBatchCommit is called on batch id: {bid} by LocalCoordinator",
                                       this.grainReference, bid);

            if (this.highestCommittedLocalBid < bid)
            {
                this.highestCommittedLocalBid = bid;
                this.transactionScheduler.GarbageCollection(this.highestCommittedLocalBid);
            }

            this.logger.LogInformation("Setting bid: {bid} to commit, these are the bid waiting: {bids}",
                                       this.grainReference, bid, string.Join(", ", this.batchCommit.Select(kv => kv.Key + " : " + kv.Value.ToString())));

            // Sets this to true, so the await in WaitForBatchCommit will continue.
            this.batchCommit[bid].SetResult(true);

            this.batchCommit.Remove(bid);
            //myScheduler.AckBatchCommit(highestCommittedBid);
            return Task.CompletedTask;
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public TState GetState<TState>(long tid, AccessMode mode, ITransactionalState<TState> state)
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

        public Task GarbageCollection(long bid)
        {
            this.transactionScheduler.GarbageCollection(bid);

            return Task.CompletedTask;
        }

        public Task CleanUp(long tid)
        {
            this.determinsticFunctionResults.Remove(tid);

            return Task.CompletedTask;
        }
    }
}