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
        private readonly int myID;
        private readonly string siloID;

        // transaction execution
        private readonly TransactionScheduler myScheduler;
        private readonly ITransactionalState<TState> state;

        // local and global coordinators
        private readonly int myLocalCoordID;
        private readonly ICoordMap coordMap;
        private readonly ILocalCoordinatorGrain myLocalCoord;
        private readonly IRegionalCoordinatorGrain regionalCoordinator;                                // use this coord to get tid for global transactions
        private readonly IGrainFactory grainFactory;

        // PACT execution
        private Dictionary<long, TaskCompletionSource<bool>> localBatchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        private Dictionary<long, BasicFuncResult> determinsticFunctionResults;       // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times

        // only for global PACT
        private Dictionary<long, long> regionalBidToLocalBid;
        private Dictionary<long, Dictionary<long, long>> regionalBidToRegionalTidToLocalTidPerBatch;    // key: global bid, <global tid, local tid>
        private Dictionary<long, TaskCompletionSource<bool>> regionalBatchInfoPromise;      // key: global bid, use to check if the SubBatch has arrived or not

        public DetTxnExecutor(
            ILogger logger,
            GrainReference grainReference,
            GrainId myId,
            int myID,
            string siloID,
            int myLocalCoordID,
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

            this.myID = myID;
            this.siloID = siloID;
            this.myLocalCoordID = myLocalCoordID;
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
        public async Task<Tuple<long, TransactionContext>> GetDetContext(List<Tuple<int, string>> grainAccessInfos, List<string> grainClassNames)
        {
            this.logger.LogInformation("Getting context for grainList: [{grainList}] and grainClassNames: [{grainClassNames}]",
                                       this.grainReference, string.Join(", ", grainAccessInfos), string.Join(", ", grainClassNames));

            // check if the transaction will access multiple silos
            var silos = new List<Tuple<int, string>>();
            var grainListPerSilo = new Dictionary<string, List<Tuple<int, string>>>();
            var grainNamePerSilo = new Dictionary<string, List<string>>();

            // This is the placement manager(PM) code described in the paper
            for (int i = 0; i < grainAccessInfos.Count; i++)
            {
                var grainId = grainAccessInfos[i];

                if (!grainListPerSilo.ContainsKey(grainId.Item2))
                {
                    silos.Add(grainId);
                    grainListPerSilo.Add(grainId.Item2, new List<Tuple<int, string>>());
                    grainNamePerSilo.Add(grainId.Item2, new List<string>());
                }

                grainListPerSilo[grainId.Item2].Add(grainId);
                grainNamePerSilo[grainId.Item2].Add(grainClassNames[i]);
            }


            // For a simple example, make sure that only 1 silo is involved in the transaction
            this.logger.LogInformation("Silolist count: {siloListCount}", this.grainReference, silos.Count);
            if (silos.Count > 1)
            {
                // get regional tid from regional coordinator
                // Note the Dictionary<string, Tuple<int, string>> part of the
                // return type of NewTransaction(..) is a map between the region
                // and which local coordinators
                Tuple<TransactionRegisterInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>> regionalInfo =
                    await this.regionalCoordinator.NewRegionalTransaction(silos);

                var regionalTid = regionalInfo.Item1.Tid;
                var regionalBid = regionalInfo.Item1.Bid;
                Dictionary<Tuple<int, string>, Tuple<int, string>> siloIDToLocalCoordID = regionalInfo.Item2;

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
                        task = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainListPerSilo[siloId.Item2], grainNamePerSilo[siloId.Item2]);
                    }
                    else
                    {
                        this.logger.LogInformation($"Is calling NewRegionalTransaction w/o task", this.grainReference);

                        _ = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainListPerSilo[siloId.Item2], grainNamePerSilo[siloId.Item2]);
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

            TransactionRegisterInfo info = await myLocalCoord.NewLocalTransaction(grainAccessInfos, grainClassNames);
            this.logger.LogInformation("Received TransactionRegisterInfo {info} from localCoordinator: {coordinator}", this.grainReference, info, this.myLocalCoord);

            var cxt2 = new TransactionContext(info.Tid, info.Bid);
            var localContext = new Tuple<long, TransactionContext>(info.HighestCommittedBid, cxt2);

            return localContext;
        }

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

                await this.localBatchInfoPromise[context.localBid].Task;

                this.logger.LogInformation("WaitForturn finished", this.grainReference);
            }

            Debug.Assert(!this.determinsticFunctionResults.ContainsKey(context.localTid));
            this.determinsticFunctionResults.Add(context.localTid, new BasicFuncResult());

            await this.myScheduler.WaitForTurn(context.localBid, context.localTid);
        }

        public async Task FinishExecuteDeterministicTransaction(TransactionContext transactionContext)
        {
            // This is the coordinatorId that sent the subbatch containing the current transaction.
            var coordinatorId = this.myScheduler.IsBatchComplete(transactionContext.localBid, transactionContext.localTid);
            this.logger.LogInformation("FinishExecuteDeterministicTransaction: the coordinator that sent the subbatch: {coordinatorId}",
                                        this.grainReference, coordinatorId);

            if (coordinatorId != -1)   // the current batch has completed on this grain
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
                                            this.grainReference, coordinatorId, localCoordinatorRegion, transactionContext.localBid);

                // TODO: This coordinator should be the one that sent the batch
                var coordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordinatorId, localCoordinatorRegion);

                _ = coordinator.AckBatchCompletion(transactionContext.localBid);
            }
        }

        public void BatchArrive(LocalSubBatch batch)
        {
            this.logger.LogInformation("Batch arrived, batch: {batch}", this.grainReference, batch);

            if (!this.localBatchInfoPromise.ContainsKey(batch.Bid))
            {
                this.localBatchInfoPromise.Add(batch.Bid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("BatchArrive: localBtchInfoPromise: {localBatchInfoPromise}", this.grainReference, localBatchInfoPromise[batch.Bid]);

            this.localBatchInfoPromise[batch.Bid].SetResult(true);

            // register global info mapping if necessary
            if (batch.RegionalBid != -1)
            {
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
