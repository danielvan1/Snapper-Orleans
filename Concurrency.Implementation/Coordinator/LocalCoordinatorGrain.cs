using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [LocalCoordinatorGrainPlacementStrategy]
    public class LocalCoordinatorGrain : Grain, ILocalCoordinatorGrain
    {
        private string region;

        // coord basic info
        private ILocalCoordinatorGrain neighborCoord;
        private Dictionary<Tuple<int, string>, string> grainIdToGrainClassName;  // grainID, grainClassName
        private readonly ILogger logger;

        // PACT
        private DetTxnProcessor detTxnProcessor;
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches;

        // Hierarchical Architecture
        // for global batches sent from global coordinators
        private SortedDictionary<long, SubBatch> regionalBatchInfo;                                   // key: regional bid
        private Dictionary<long, Dictionary<long, List<Tuple<int, string>>>> regionalTransactionInfo; // <regional bid, <regional tid, grainAccessInfo>>
        private Dictionary<long, TaskCompletionSource<Tuple<long, long>>> regionalDetRequestPromise;  // <regional tid, <local bid, local tid>>
        private Dictionary<long, long> localBidToRegionalBid;
        private Dictionary<long, Dictionary<long, long>> regionalTidToLocalTidPerBatch;               // local bid, <regional tid, local tid>

        // for regional batch commitment
        private long highestCommittedRegionalBid;
        private Dictionary<long, long> regionalBidToRegionalCoordID;
        private Dictionary<long, bool> regionalBidToIsPreviousBatchRegional;                                // regional bid, if this batch's previous one is also a regional batch
        private Dictionary<long, TaskCompletionSource<bool>> regionalBatchCommit;                     // regional bid, commit promise

        public LocalCoordinatorGrain(ILogger<LocalCoordinatorGrain> logger)
        {
            this.logger = logger;
        }

        #region Activation
        public override Task OnActivateAsync()
        {
            this.Init();

            this.detTxnProcessor = new DetTxnProcessor(
                this.logger,
                this.GrainReference,
                this.GetPrimaryKeyLong(out _),
                this.expectedAcksPerBatch,
                this.bidToSubBatches,
                this.GrainFactory
                );

            this.GetPrimaryKeyLong(out string region);
            this.region = region;

            return base.OnActivateAsync();
        }

        public Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor)
        {
            this.highestCommittedRegionalBid = -1;
            this.detTxnProcessor.Init();

            this.neighborCoord = neighbor;

            return Task.CompletedTask;
        }

        private void Init()
        {
            this.highestCommittedRegionalBid = -1;
            this.grainIdToGrainClassName = new Dictionary<Tuple<int, string>, string>();
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>>();

            this.regionalBatchInfo = new SortedDictionary<long, SubBatch>();
            this.regionalTransactionInfo = new Dictionary<long, Dictionary<long, List<Tuple<int, string>>>>();
            this.regionalDetRequestPromise = new Dictionary<long, TaskCompletionSource<Tuple<long, long>>>();
            this.localBidToRegionalBid = new Dictionary<long, long>();
            this.regionalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            this.regionalBidToIsPreviousBatchRegional = new Dictionary<long, bool>();
            this.regionalBatchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.regionalBidToRegionalCoordID = new Dictionary<long, long>();
        }
        #endregion

        #region NewTransaction from TransactionExecutionGrain

        // for PACT
        public async Task<TransactionRegisterInfo> NewLocalTransaction(List<Tuple<int, string>> grainAccessInfo, List<string> grainClassNames)
        {
            this.logger.LogInformation("NewLocalTransaction is called with grainAccessInfo: {grainAccessInfo}, grainClassNames: {grainClassNames}",
                                       this.GrainReference, string.Join(", ", grainAccessInfo), string.Join(", ", grainIdToGrainClassName));

            Task<Tuple<long, long>> getBidAndTidTask = this.detTxnProcessor.GetDeterministicTransactionBidAndTid(grainAccessInfo);

            // Mapping each grainId to the corresponding grainClassName
            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grainId = grainAccessInfo[i];

                if (!this.grainIdToGrainClassName.ContainsKey(grainId))
                {
                    this.grainIdToGrainClassName.Add(grainId, grainClassNames[i]);
                }
            }

            var bidAndTid = await getBidAndTidTask;
            long bid = bidAndTid.Item1;
            long tid = bidAndTid.Item2;

            this.logger.LogInformation("NewTransaction is going to return bid: {bid} and tid: {tid}", this.GrainReference, bid, tid);

            return new TransactionRegisterInfo(bid, tid, this.detTxnProcessor.highestCommittedBid);
        }

        public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName)
        {
            this.logger.LogInformation("NewRegionalTransaction is called regionalBid {globalBid} and regionalTid {tid}, with grainAccessInfo {grainAccesInfo}",
                                        this.GrainReference, regionalBid, regionalTid, string.Join(", ", grainAccessInfo));

            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grainID = grainAccessInfo[i];

                if (!this.grainIdToGrainClassName.ContainsKey(grainID))
                {
                    this.grainIdToGrainClassName.Add(grainID, grainClassName[i]);
                }
            }

            if (!this.regionalTransactionInfo.ContainsKey(regionalBid))
            {
                this.regionalTransactionInfo.Add(regionalBid, new Dictionary<long, List<Tuple<int, string>>>());
            }

            this.regionalTransactionInfo[regionalBid].Add(regionalTid, grainAccessInfo);

            var promise = new TaskCompletionSource<Tuple<long, long>>();
            this.regionalDetRequestPromise.Add(regionalTid, promise);
            await promise.Task;

            return new TransactionRegisterInfo(promise.Task.Result.Item1, promise.Task.Result.Item2, this.detTxnProcessor.highestCommittedBid);
        }

        #endregion

        /// <summary>
        /// This method is used to pass the token around a ring of coordinators.
        /// Currently we are following a token ring algorithm. The downside of this
        /// way is that if we do not have any transactions in the system, the passing
        /// of tokens will use an extreme amount of the resources. This can cause problems
        /// if the load of work is small and the interval between transactions are more than
        /// a second.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task PassToken(LocalToken token)
        {
            long curBatchID;
            IList<long> curBatchIDs = new List<long>();

            // TODO: Why do we need to do it like this?
            if (token.isLastEmitBidGlobal)
            {
                curBatchIDs = this.GenerateRegionalBatch(token);
                curBatchID = this.detTxnProcessor.GenerateBatch(token);
            }
            else
            {
                curBatchID = this.detTxnProcessor.GenerateBatch(token);
                curBatchIDs = this.GenerateRegionalBatch(token);
            }

            if (this.detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
            {
                this.detTxnProcessor.GarbageCollectTokenInfo(token);
            }
            else
            {
                this.detTxnProcessor.highestCommittedBid = token.highestCommittedBid;
            }

            //this.logger.LogInformation("Pass token is called", this.GrainReference);
            await Task.Delay(2);
            _ = this.neighborCoord.PassToken(token);
            if (curBatchID != -1) await EmitBatch(curBatchID);
            if (curBatchIDs.Count != 0)
            {
                foreach (var bid in curBatchIDs)
                {
                    await EmitBatch(bid);
                }
            }
        }

        /// <summary>
        /// This is called from the TransactionExecutionGrain when the grain is done with its transactions in its current subbatch.
        /// If this is a regional transaction we will then wait for commit confirmation from the corresponding regional coordinator.
        /// If this is a local transaction we will wait until previous batch is committed.
        /// Also responsible to sent the commit confirmation to the TransactionExecutionGrains.
        /// </summary>
        /// <param name="bid"></param>
        /// <returns></returns>
        public async Task AckBatchCompletion(long bid)
        {
            this.logger.LogInformation("Expected acknowledgements for batch: {bid} before decrement: {expectedAcksPerBatch}", this.GrainReference, bid, this.expectedAcksPerBatch[bid]);
            this.expectedAcksPerBatch[bid]--;

            if (this.expectedAcksPerBatch[bid] > 0) return;


            // the batch has been completed in this silo
            long regionalBid = -1;

            // TODO: Why does it matter why the previous is a regional or local batch?
            bool isPreviousRegional = false;
            bool isRegional = this.localBidToRegionalBid.ContainsKey(bid);

            this.logger.LogInformation("Got all acknowledgements for batch: {bid}. Is the batch regional: {isRegional} and is the previous batch regional: {isPreviousRegional}",
                                        this.GrainReference, bid, isRegional, isPreviousRegional);

            // Some sick optimization.......
            if (isRegional)
            {
                // ACK the regional coordinator
                regionalBid = this.localBidToRegionalBid[bid];
                isPreviousRegional = this.regionalBidToIsPreviousBatchRegional[regionalBid];

                if (isPreviousRegional)
                {
                    this.AckCompletionToRegionalCoordinator(regionalBid);

                }
            }

            await this.detTxnProcessor.WaitPrevBatchToCommit(bid);

            if (isRegional)
            {
                if (!isPreviousRegional)
                {
                    this.AckCompletionToRegionalCoordinator(regionalBid);
                }

                await this.WaitRegionalBatchCommit(regionalBid);

                this.localBidToRegionalBid.Remove(bid);
                this.regionalBidToRegionalCoordID.Remove(regionalBid);
                this.regionalBidToIsPreviousBatchRegional.Remove(regionalBid);
            }

            this.detTxnProcessor.AckBatchCommit(bid);

            Dictionary<Tuple<int, string>, SubBatch> currentScheduleMap = bidToSubBatches[bid];

            // Sent message that the transaction grains can commit
            foreach ((Tuple<int, string> grainId, SubBatch subBatch) in currentScheduleMap)
            {
                this.GetPrimaryKeyLong(out string region);
                this.logger.LogInformation($"Commit Grains", this.GrainReference);
                Debug.Assert(region == grainId.Item2); // I think this should be true, we just have the same info multiple places now
                var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Item1, region, this.grainIdToGrainClassName[grainId]);
                _ = destination.AckBatchCommit(bid);
            }

            this.bidToSubBatches.Remove(bid);
            this.expectedAcksPerBatch.Remove(bid);
        }

        /// <summary>
        /// This method is needed since we want to wait for the batch to commit in another local grains dettxnprocessor.
        /// Since each dettxnprocessor can not talk to each other we need this method.
        /// </summary>
        /// <param name="bid"></param>
        /// <returns></returns>
        public async Task WaitBatchCommit(long bid)
        {
            await this.detTxnProcessor.WaitBatchCommit(bid);
        }


        #region Communication with RegionalCoordinators

        // Going to be called by our Regional Coordinator
        public Task ReceiveBatchSchedule(SubBatch batch)
        {
            this.logger.LogInformation("Received batch schedule from regional coordinator {regionalCoordinatorId} with previous bid {previousBatchId} and current bid {bid}",
                                       this.GrainReference, batch.CoordinatorId, batch.PreviousBid, batch.Bid);

            var regionalBid = batch.Bid;
            this.regionalBatchInfo.Add(regionalBid, batch);
            this.regionalBidToRegionalCoordID.Add(regionalBid, batch.CoordinatorId);

            if (!this.regionalTransactionInfo.ContainsKey(regionalBid))
            {
                this.regionalTransactionInfo.Add(regionalBid, new Dictionary<long, List<Tuple<int, string>>>());
            }

            return Task.CompletedTask;
        }

        public Task AckRegionalBatchCommit(long regionalBid)
        {
            this.logger.LogInformation("AckRegionalBatch commit was called from regional coordinator. We can now commit batch: {regionalBid}",
                                       this.GrainReference,  regionalBid);

            this.highestCommittedRegionalBid = Math.Max(regionalBid, highestCommittedRegionalBid);

            if (this.regionalBatchCommit.ContainsKey(regionalBid))
            {
                this.regionalBatchCommit[regionalBid].SetResult(true);
                this.regionalBatchCommit.Remove(regionalBid);
            }

            return Task.CompletedTask;
        }

        private IList<long> GenerateRegionalBatch(LocalToken token)
        {
            IList<long> currentBatchIds = new List<long>();

            while (this.regionalBatchInfo.Count > 0)
            {
                // this.logger.LogInformation("ProcessingRegionalBatch: Received the local token and we have received current regional sub batches: {regionalBatchInfo}",
                //                            string.Join(", ", this.regionalBatchInfo) );

                var bidAndBatch = this.regionalBatchInfo.First();
                var regionalBid = bidAndBatch.Key;
                var subBatch = bidAndBatch.Value;

                if (subBatch.PreviousBid != token.lastEmitGlobalBid) return new List<long>();
                if (subBatch.Transactions.Count != this.regionalTransactionInfo[regionalBid].Count) return new List<long>();

                var currentBatchId = token.lastEmitTid + 1;
                currentBatchIds.Add(currentBatchId);
                this.localBidToRegionalBid.Add(currentBatchId, regionalBid);
                this.regionalTidToLocalTidPerBatch.Add(currentBatchId, new Dictionary<long, long>());

                foreach (var globalTid in subBatch.Transactions)
                {
                    var localTid = ++token.lastEmitTid;
                    this.regionalDetRequestPromise[globalTid].SetResult(new Tuple<long, long>(currentBatchId, localTid));

                    var grainAccessInfo = this.regionalTransactionInfo[regionalBid][globalTid];
                    this.detTxnProcessor.GenerateSchedulePerService(localTid, currentBatchId, grainAccessInfo);

                    this.regionalTidToLocalTidPerBatch[currentBatchId].Add(globalTid, localTid);
                    this.regionalDetRequestPromise.Remove(globalTid);
                }

                this.regionalBidToIsPreviousBatchRegional.Add(regionalBid, token.isLastEmitBidGlobal);
                this.regionalBatchInfo.Remove(regionalBid);
                this.regionalTransactionInfo.Remove(regionalBid);
                this.detTxnProcessor.UpdateToken(token, currentBatchId, regionalBid);
                token.lastEmitGlobalBid = regionalBid;
            }

            return currentBatchIds;
        }

        private void AckCompletionToRegionalCoordinator(long regionalBid)
        {
            this.GetPrimaryKeyLong(out string region);
            var regionalCoordID = this.regionalBidToRegionalCoordID[regionalBid];
            // Just try to get the regional silo somehow to see if it works
            string regionalCoordinatorRegion = region.Substring(0, 2);
            this.logger.LogInformation("Complete current regional batch: {regionalBid} to RegionalCoordinator {id}-{region}", this.GrainReference, regionalBid, regionalCoordID, regionalCoordinatorRegion);
            //this.logger.Info($"[{region}] LocalCoordinatorGrain is going to call AckBatchCompletion on the regional coordinator:{regionalCoordinatorRegion} ID:{globalCoordID}");
            var regionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordID, regionalCoordinatorRegion);

            _ = regionalCoordinator.AckBatchCompletion(regionalBid);
        }

        private async Task WaitRegionalBatchCommit(long regionalBid)
        {
            if (this.highestCommittedRegionalBid >= regionalBid)
            {
                return;
            }
            if (!this.regionalBatchCommit.ContainsKey(regionalBid))
            {
                this.regionalBatchCommit.Add(regionalBid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("Waiting for the regional batch: {bid} to commit",
                                        this.GrainReference, regionalBid);

            await this.regionalBatchCommit[regionalBid].Task;
        }

        #endregion

        private async Task EmitBatch(long bid)
        {
            Dictionary<Tuple<int, string>, SubBatch> currentScheduleMap = this.bidToSubBatches[bid];

            long globalBid = -1;
            if (this.localBidToRegionalBid.ContainsKey(bid))
            {
                globalBid = this.localBidToRegionalBid[bid];
            }

            var regionalTidToLocalTid = new Dictionary<long, long>();
            if (this.regionalTidToLocalTidPerBatch.ContainsKey(bid))
            {
                regionalTidToLocalTid = this.regionalTidToLocalTidPerBatch[bid];
                this.regionalTidToLocalTidPerBatch.Remove(bid);
            }

            foreach (( Tuple<int, string> grainId, SubBatch subBatch) in currentScheduleMap)
            {
                int id = grainId.Item1;
                string region = grainId.Item2;

                this.logger.LogInformation("Calling EmitBatch on transaction execution grain: {grainId}", this.GrainReference, grainId);

                // I think this should be true, we just have the same info multiple places now
                // The problem is if this is not true, then the local coordinator is talking to
                // grains in other servers

                var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(id, region, this.grainIdToGrainClassName[grainId]);

                var localSubBatch = new LocalSubBatch(subBatch)
                {
                    RegionalBid = globalBid,
                    HighestCommittedBid = this.detTxnProcessor.highestCommittedBid,
                    RegionalTidToLocalTid = regionalTidToLocalTid
                };

                _ = destination.ReceiveBatchSchedule(localSubBatch);
            }
        }
    }
}
