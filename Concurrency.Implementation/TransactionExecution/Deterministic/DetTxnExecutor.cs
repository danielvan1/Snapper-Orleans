using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
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
        private TransactionScheduler myScheduler;
        private ITransactionalState<TState> state;

        // local and global coordinators
        private readonly int myLocalCoordID;
        private readonly ICoordMap coordMap;
        private readonly ILocalCoordinatorGrain myLocalCoord;
        private readonly IRegionalCoordinatorGrain myRegionalCoordinator;                                // use this coord to get tid for global transactions
        private readonly IGrainFactory grainFactory;

        // PACT execution
        private Dictionary<long, TaskCompletionSource<bool>> localBatchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        private Dictionary<long, BasicFuncResult> determinsticFunctionResults;       // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times

        // only for global PACT
        private Dictionary<long, long> regionalBidToLocalBid;
        private Dictionary<long, Dictionary<long, long>> regionalTidToLocalTidPerBatch;    // key: global bid, <global tid, local tid>
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
            this.myRegionalCoordinator = myRegionalCoordinator;
            this.grainFactory = grainFactory;
            this.myScheduler = myScheduler;
            this.state = state;

            this.localBatchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
            this.determinsticFunctionResults = new Dictionary<long, BasicFuncResult>();
            this.regionalBidToLocalBid = new Dictionary<long, long>();
            this.regionalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            this.regionalBatchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // int: the highestCommittedBid get from local coordinator
        public async Task<Tuple<long, TransactionContext>> GetDetContext(List<Tuple<int, string>> grainList, List<string> grainClassNames)
        {
            this.logger.LogInformation("Getting context for grainList: [{grainList}] and grainClassNames: [{grainClassNames}]",
                                       this.grainReference, string.Join(", ", grainList), string.Join(", ", grainClassNames));

            if (Constants.multiSilo && Constants.hierarchicalCoord)
            {
                // check if the transaction will access multiple silos
                var siloList = new List<Tuple<int, string>>();
                var grainListPerSilo = new Dictionary<string, List<Tuple<int, string>>>();
                var grainNamePerSilo = new Dictionary<string, List<string>>();

                // This is the placement manager(PM) code described in the paper
                for (int i = 0; i < grainList.Count; i++)
                {
                    var grainID = grainList[i];
                    if (grainListPerSilo.ContainsKey(grainID.Item2) == false)
                    {
                        siloList.Add(grainID);
                        grainListPerSilo.Add(grainID.Item2, new List<Tuple<int, string>>());
                        grainNamePerSilo.Add(grainID.Item2, new List<string>());
                    }

                    grainListPerSilo[grainID.Item2].Add(grainID);
                    grainNamePerSilo[grainID.Item2].Add(grainClassNames[i]);
                }

                // For a simple example, make sure that only 1 silo is involved in the transaction
                this.logger.LogInformation("Silolist count: {siloListCount}", this.grainReference, siloList.Count);
                if (siloList.Count != 1)
                {
                    // get regional tid from regional coordinator
                    // TODO: Should be our Regional Coordinators here.
                    // Note the Dictionary<string, Tuple<int, string>> part of the
                    // return type of NewTransaction(..) is a map between the region
                    // and which local coordinators
                    Tuple<TransactionRegisterInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>> regionalInfo = await myRegionalCoordinator.NewTransaction(siloList);
                    var regionalTid = regionalInfo.Item1.Tid;
                    var regionalBid = regionalInfo.Item1.Bid;
                    var siloIDToLocalCoordID = regionalInfo.Item2;

                    // send corresponding grainAccessInfo to local coordinators in different silos
                    Debug.Assert(grainListPerSilo.ContainsKey(siloID));
                    Task<TransactionRegisterInfo> task = null;
                    for (int i = 0; i < siloList.Count; i++)
                    {
                        var siloID = siloList[i];
                        Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloID));

                        // TODO: Need a map from coordinator to local coordinator
                        var coordID = siloIDToLocalCoordID[siloID];
                        var localCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordID.Item1, coordID.Item2);

                        // get local tid, bid from local coordinator
                        if (coordID.Item2 == this.siloID)
                        {
                            this.logger.LogInformation($"Is calling NewGlobalTransaction w/ task", this.grainReference);
                            task = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainListPerSilo[siloID.Item2], grainNamePerSilo[siloID.Item2]);
                        }
                        else
                        {
                            this.logger.LogInformation($"Is calling NewGlobalTransaction w/o task", this.grainReference);

                            _ = localCoordinator.NewRegionalTransaction(regionalBid, regionalTid, grainListPerSilo[siloID.Item2], grainNamePerSilo[siloID.Item2]);
                        }
                    }


                    Debug.Assert(task != null);
                    this.logger.LogInformation($"Waiting for task in GetDetContext", this.grainReference);
                    TransactionRegisterInfo localInfo = await task;
                    this.logger.LogInformation($"Is DONE waiting for task in GetDetContext, going to return tx context", this.grainReference);
                    var cxt1 = new TransactionContext(localInfo.Bid, localInfo.Tid, regionalBid, regionalTid);

                    // TODO: What is this -1??
                    return new Tuple<long, TransactionContext>(-1, cxt1) ;
                }
            }

            TransactionRegisterInfo info = await myLocalCoord.NewTransaction(grainList, grainClassNames);
            this.logger.LogInformation("Received TransactionRegisterInfo {info} from localCoordinator: {coordinator}", this.grainReference, info, this.myLocalCoord);

            var cxt2 = new TransactionContext(info.Tid, info.Bid);
            var context = new Tuple<long, TransactionContext>(info.HighestCommittedBid, cxt2);

            return context;
        }

        public async Task WaitForTurn(TransactionContext context)
        {
            // check if it is a global PACT
            if (context.globalBid != -1)
            {
                // wait until the SubBatch has arrived this grain
                if (regionalBatchInfoPromise.ContainsKey(context.globalBid) == false)
                    regionalBatchInfoPromise.Add(context.globalBid, new TaskCompletionSource<bool>());
                await regionalBatchInfoPromise[context.globalBid].Task;

                // need to map global info to the corresponding local tid and bid
                context.localBid = regionalBidToLocalBid[context.globalBid];
                context.localTid = regionalTidToLocalTidPerBatch[context.globalBid][context.globalTid];
            }
            else
            {
                this.logger.LogInformation("WaitForturn waiting", this.grainReference);
                // wait until the SubBatch has arrived this grain
                if (localBatchInfoPromise.ContainsKey(context.localBid) == false)
                    localBatchInfoPromise.Add(context.localBid, new TaskCompletionSource<bool>());
                await localBatchInfoPromise[context.localBid].Task;

                this.logger.LogInformation("WaitForturn finished", this.grainReference);
            }

            Debug.Assert(determinsticFunctionResults.ContainsKey(context.localTid) == false);
            determinsticFunctionResults.Add(context.localTid, new BasicFuncResult());

            await myScheduler.WaitForTurn(context.localBid, context.localTid);
        }

        public async Task FinishExecuteDeterministicTransaction(TransactionContext transactionContext)
        {
            // This is the coordinatorId that sent the subbatch containing the current transaction.
            var coordinatorId = this.myScheduler.IsBatchComplete(transactionContext.localBid, transactionContext.localTid);

            if (coordinatorId != -1)   // the current batch has completed on this grain
            {
                this.localBatchInfoPromise.Remove(transactionContext.localBid);

                if (transactionContext.globalBid != -1)
                {
                    this.regionalBidToLocalBid.Remove(transactionContext.globalBid);
                    this.regionalTidToLocalTidPerBatch.Remove(transactionContext.globalBid);
                    this.regionalBatchInfoPromise.Remove(transactionContext.globalBid);
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
            if (batch.GlobalBid != -1)
            {
                this.regionalBidToLocalBid.Add(batch.GlobalBid, batch.Bid);
                this.regionalTidToLocalTidPerBatch.Add(batch.GlobalBid, batch.GlobalTidToLocalTid);

                if (this.regionalBatchInfoPromise.ContainsKey(batch.GlobalBid) == false)
                    this.regionalBatchInfoPromise.Add(batch.GlobalBid, new TaskCompletionSource<bool>());
                this.regionalBatchInfoPromise[batch.GlobalBid].SetResult(true);
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
