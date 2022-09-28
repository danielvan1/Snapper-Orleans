using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
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


        private Random random;
        private long myId;
        private readonly bool isRegionalCoordinator = false;
        public long highestCommittedBid;

        // transaction processing
        private List<List<Tuple<int, string>>> deterministicRequests;
        private List<TaskCompletionSource<Tuple<long, long>>> deterministicRequestPromise; // <local bid, local tid>

        // batch processing
        private Dictionary<long, long> bidToLastBid;
        private Dictionary<long, long> bidToLastCoordID; // <bid, coordID who emit this bid's lastBid>
        private Dictionary<long, TaskCompletionSource<bool>> batchCommit;

        // only for global batch
        private Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch; // regional bid, silo ID, chosen local coord ID

        public LocalCoordinatorGrain(ILogger<LocalCoordinatorGrain> logger)
        {
            this.logger = logger;
        }

        #region Activation
        public override Task OnActivateAsync()
        {
            this.Init();

            this.random = new Random();
            this.myId = this.GetPrimaryKeyLong(out string region);

            this.region = region;

            return base.OnActivateAsync();
        }

        public Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor)
        {

            this.neighborCoord = neighbor;

            return Task.CompletedTask;
        }

        private void Init()
        {
            this.highestCommittedRegionalBid = -1;
            this.highestCommittedBid = -1;
            this.deterministicRequests = new List<List<Tuple<int, string>>>();
            this.deterministicRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            // this.highestCommittedRegionalBid = -1;
            this.grainIdToGrainClassName = new Dictionary<Tuple<int, string>, string>();
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>>();
            this.bidToLastBid = new Dictionary<long, long>();
            this.bidToLastCoordID = new Dictionary<long, long>(); // <bid, coordID who emit this bid's lastBid>

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
                                       this.GrainReference, string.Join(", ", grainAccessInfo), string.Join(", ", grainClassNames));

            Task<Tuple<long, long>> getBidAndTidTask = this.GetDeterministicTransactionBidAndTid(grainAccessInfo);

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

            this.logger.LogInformation("NewLocalTransaction is going to return bid: {bid} and tid: {tid}", this.GrainReference, bid, tid);

            return new TransactionRegisterInfo(bid, tid, this.highestCommittedBid);
        }

        public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName)
        {
            this.logger.LogInformation("NewRegionalTransaction is called regionalBid {globalBid} and regionalTid {tid}", this.GrainReference, regionalBid, regionalTid);

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
            this.logger.LogInformation("Waiting for promise {promise} to bet set to a value", this.GrainReference, promise);
            this.regionalDetRequestPromise.Add(regionalTid, promise);
            await promise.Task;

            this.logger.LogInformation("Done waiting for promise {promise} to bet set to a value", this.GrainReference, promise);

            return new TransactionRegisterInfo(promise.Task.Result.Item1, promise.Task.Result.Item2, highestCommittedBid);
        }

        public async Task<Tuple<long, long>> GetDeterministicTransactionBidAndTid(List<Tuple<int, string>> serviceList)   // returns a Tuple<bid, tid>
        {
            this.deterministicRequests.Add(serviceList);
            var promise = new TaskCompletionSource<Tuple<long, long>>();

            // We are waiting until the token is arrived which then will create the tuple.
            this.deterministicRequestPromise.Add(promise);

            this.logger.LogInformation("Waiting for the token to arrive", this.GrainReference);
            var tuple = await promise.Task;

            long bid = tuple.Item1;
            long tid = tuple.Item2;

            this.logger.LogInformation("Token has arrived and sat the bid: {bid} and tid: {tid} ", this.GrainReference, bid, tid);

            return new Tuple<long, long>(bid, tid);
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
            Thread.Sleep(10);

            // TODO: Why do we need to do it like this?
            if (token.IsLastEmitBidGlobal)
            {
                curBatchIDs = this.GenerateRegionalBatch(token);
                curBatchID = this.GenerateBatch(token);
            }
            else
            {
                curBatchID = this.GenerateBatch(token);
                curBatchIDs = this.GenerateRegionalBatch(token);
            }

            if (this.highestCommittedBid > token.HighestCommittedBid)
            {
                GarbageCollectTokenInfo(token);
            }
            else
            {
                this.highestCommittedBid = token.HighestCommittedBid;
            }

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

            await this.WaitPrevBatchToCommit(bid);

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

            this.AckBatchCommit(bid);

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
        /// TODO: Explain why we need to wait for cimmit in another coordinator
        /// Since each dettxnprocessor can not talk to each other we need this method.
        /// </summary>
        /// <param name="bid"></param>
        /// <returns></returns>
        public async Task WaitBatchCommit(long bid)
        {
            if (this.highestCommittedBid == bid) return;

            if (!this.batchCommit.ContainsKey(bid))
            {
                this.batchCommit.Add(bid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("Waiting for batch: {bid} to commit", this.GrainReference, bid);

            await this.batchCommit[bid].Task;

            this.logger.LogInformation("Finish waiting for batch: {bid} to commit", this.GrainReference, bid);
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

        /// <summary>
        /// This is called from the <see cref="RegionalCoordinatorGrain"/> to notify that the current batch id is
        /// ready for commit.
        /// </summary>
        /// <param name="regionalBid">bid to commit</param>
        /// <returns></returns>
        public Task AckRegionalBatchCommit(long regionalBid)
        {
            this.logger.LogInformation("AckRegionalBatch commit was called from regional coordinator. We can now commit batch: {regionalBid}",
                                       this.GrainReference,  regionalBid);

            this.highestCommittedRegionalBid = Math.Max(regionalBid, this.highestCommittedRegionalBid);

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
                var bidAndBatch = this.regionalBatchInfo.First();
                var regionalBid = bidAndBatch.Key;
                var subBatch = bidAndBatch.Value;

                // this.logger.LogInformation("ProcessingRegionalBatch: Received the local token and we have received current regional sub batches: {regionalBatchInfo}:  Subbatch{subbatch}",
                //                             this.GrainReference, string.Join(", ", this.regionalBatchInfo.Select(kv => kv.Key + " : " + kv.Value)), subBatch);

                if (subBatch.PreviousBid != token.lastEmitGlobalBid)
                {
                    return new List<long>();
                }

                if (subBatch.Transactions.Count != this.regionalTransactionInfo[regionalBid].Count)
                {
                    return new List<long>();
                }

                this.logger.LogInformation("HerpDerp", this.GrainReference);

                var currentBatchId = token.PreviousEmitTid + 1;
                currentBatchIds.Add(currentBatchId);
                this.localBidToRegionalBid.Add(currentBatchId, regionalBid);
                this.regionalTidToLocalTidPerBatch.Add(currentBatchId, new Dictionary<long, long>());

                foreach (var globalTid in subBatch.Transactions)
                {
                    var localTid = ++token.PreviousEmitTid;
                    this.regionalDetRequestPromise[globalTid].SetResult(new Tuple<long, long>(currentBatchId, localTid));

                    var grainAccessInfo = this.regionalTransactionInfo[regionalBid][globalTid];
                    GenerateSchedulePerService(localTid, currentBatchId, grainAccessInfo);

                    this.regionalTidToLocalTidPerBatch[currentBatchId].Add(globalTid, localTid);
                    this.regionalDetRequestPromise.Remove(globalTid);
                }

                this.regionalBidToIsPreviousBatchRegional.Add(regionalBid, token.IsLastEmitBidGlobal);
                this.regionalBatchInfo.Remove(regionalBid);
                this.regionalTransactionInfo.Remove(regionalBid);
                UpdateToken(token, currentBatchId, regionalBid);
                token.lastEmitGlobalBid = regionalBid;
            }

            return currentBatchIds;
        }

        private void AckCompletionToRegionalCoordinator(long regionalBid)
        {
            this.GetPrimaryKeyLong(out string region);
            string region1 = new string(region);
            var regionalCoordID = this.regionalBidToRegionalCoordID[regionalBid];
            // Just try to get the regional silo somehow to see if it works
            string regionalCoordinatorRegion = region1.Substring(0, 2);
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

            long regionalBid = -1;
            if (this.localBidToRegionalBid.ContainsKey(bid))
            {
                regionalBid = this.localBidToRegionalBid[bid];
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
                    RegionalBid = regionalBid,
                    HighestCommittedBid = highestCommittedBid,
                    RegionalTidToLocalTid = regionalTidToLocalTid
                };

                _ = destination.ReceiveBatchSchedule(localSubBatch);
            }
        }

        #region Processor

        /// <summary>
        /// This is called every time the corresponding coordinator receives the token.
        /// </summary>
        /// <returns>batchId</returns>
        public long GenerateBatch(TokenBase token)
        {
            if (this.deterministicRequests.Count == 0)
            {
                return -1;
            }

            this.logger.LogInformation("Generate batch2: {size}", this.GrainReference, this.deterministicRequests.Count);

            // assign bid and tid to waited PACTs
            var currentBatchID = token.PreviousEmitTid + 1;

            for (int i = 0; i < this.deterministicRequests.Count; i++)
            {
                var tid = ++token.PreviousEmitTid;
                this.GenerateSchedulePerService(tid, currentBatchID, this.deterministicRequests[i]);
                this.logger.LogInformation("Herp", this.GrainReference);
                var herp = new Tuple<long, long>(currentBatchID, tid);
                this.deterministicRequestPromise[i].SetResult(herp);
                this.logger.LogInformation("setting tuple: {herp} to a value", this.GrainReference, herp);
            }

            this.UpdateToken(token, currentBatchID, -1);

            this.deterministicRequests.Clear();
            this.deterministicRequestPromise.Clear();

            return currentBatchID;
        }

        // GenerateSchedulePerService could be called as a:
        //
        // - Regional coordinator (Then 'service' refers to local coordinator)
        // - Local coordinator (Then 'service' refers to execution grains inside silo)
        //
        // This is why we differentiate between the two with isRegionalCoordinator
        // This method creates the subbatch for each of the executiongrains
        public void GenerateSchedulePerService(long tid, long currentBatchId, List<Tuple<int, string>> deterministicRequests)
        {
            this.logger.LogInformation("GenerateSchedulePerService", this.GrainReference);
            if (!this.bidToSubBatches.ContainsKey(currentBatchId))
            {
                this.bidToSubBatches.Add(currentBatchId, new Dictionary<Tuple<int, string>, SubBatch>());
                if (this.isRegionalCoordinator)
                {
                    // Maps: currentbatchID => <region, <local coordinator Tuple(id, region)>>
                    this.localCoordinatorPerSiloPerBatch.Add(currentBatchId, new Dictionary<Tuple<int, string>, Tuple<int, string>>());
                }
            }

            Dictionary<Tuple<int, string>, SubBatch> deterministicRequestToSubBatch = this.bidToSubBatches[currentBatchId];

            for (int i = 0; i < deterministicRequests.Count; i++)
            {
                var grainId = deterministicRequests[i];

                if (!deterministicRequestToSubBatch.ContainsKey(grainId))
                {
                    deterministicRequestToSubBatch.Add(grainId, new SubBatch(currentBatchId, myId));

                    if (this.isRegionalCoordinator)
                    {
                        // randomly choose a local coord as the coordinator for this batch on that silo
                        int randomlyChosenLocalCoordinatorID = this.random.Next(Constants.numLocalCoordPerSilo);
                        string localCoordinatorSilo = grainId.Item2;
                        this.localCoordinatorPerSiloPerBatch[currentBatchId].Add(grainId, new Tuple<int, string>(randomlyChosenLocalCoordinatorID, localCoordinatorSilo));
                    }
                }

                // TODO: This seems pretty sketchy. Why do we add the same tid so many times?
                deterministicRequestToSubBatch[grainId].Transactions.Add(tid);
            }
        }

        public void UpdateToken(TokenBase token, long currentBatchId, long globalBid)
        {
            // Here we assume that every actor is only called once
            Dictionary<Tuple<int, string>, SubBatch> serviceIDToSubBatch = this.bidToSubBatches[currentBatchId];
            this.expectedAcksPerBatch.Add(currentBatchId, serviceIDToSubBatch.Count);
            this.logger.LogInformation("UpdateToken: for current batch: {bid} and token: {token}", this.GrainReference, currentBatchId, token);

            // update the previous batch ID for each service accessed by this batch
            foreach (var serviceInfo in serviceIDToSubBatch)
            {
                Tuple<int, string> serviceId = serviceInfo.Key;
                SubBatch subBatch = serviceInfo.Value;
                this.logger.LogInformation("service: {service} and subbatch: {subbatch}", this.GrainReference, serviceId, subBatch);

                if (token.previousBidPerService.ContainsKey(serviceId))
                {
                    this.logger.LogInformation("New subbatch previousBid value: {value}", this.GrainReference, token.previousBidPerService[serviceId]);
                    subBatch.PreviousBid = token.previousBidPerService[serviceId];
                    // TODO: Consider this: token.lastGlobalBidPerGrain[new Tuple<int, string>(this.myID, serviceID)];
                    // the old code just used token.lastGlobalBidPerGrain[serviceID];
                    if (!this.isRegionalCoordinator)
                    {
                        subBatch.previousGlobalBid = token.previousRegionalBidPerGrain[serviceId];
                    }
                }
                // else, the default value is -1

                Debug.Assert(subBatch.Bid > subBatch.PreviousBid);
                token.previousBidPerService[serviceId] = subBatch.Bid;
                if (!this.isRegionalCoordinator)
                {
                    token.previousRegionalBidPerGrain[serviceId] = globalBid;
                }
            }
            this.bidToLastBid.Add(currentBatchId, token.PreviousEmitBid);

            if (token.PreviousEmitBid != -1)
            {
                this.bidToLastCoordID.Add(currentBatchId, token.PreviousCoordID);
            }

            token.PreviousEmitBid = currentBatchId;
            token.IsLastEmitBidGlobal = globalBid != -1;
            token.PreviousCoordID = this.myId;

            this.logger.LogInformation("updated token: {token}", this.GrainReference, token);
        }

        public async Task WaitPrevBatchToCommit(long bid)
        {
            var previousBid = this.bidToLastBid[bid];
            this.logger.LogInformation("Waiting for previous batch: {prevBid} to commit. Current bid: {bid}", this.GrainReference, previousBid, bid);
            this.bidToLastBid.Remove(bid);

            if (this.highestCommittedBid < previousBid)
            {
                var coordinator = this.bidToLastCoordID[bid];
                if (coordinator == this.myId)
                {
                    await this.WaitBatchCommit(previousBid);
                }
                else
                {
                    this.logger.LogInformation("FUCKING HERP DERP", this.GrainReference);
                    if (this.isRegionalCoordinator)
                    {
                        this.GrainReference.GetPrimaryKeyLong(out string region);
                        string regionalCoordinatorRegion = region.Substring(0, 2);
                        var previousBatchRegionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(coordinator, regionalCoordinatorRegion);
                        await previousBatchRegionalCoordinator.WaitBatchCommit(previousBid);
                    }
                    else // if it is a local coordinator
                    {
                        this.GrainReference.GetPrimaryKeyLong(out string region);
                        var previousBatchCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(coordinator, region);
                        await previousBatchCoordinator.WaitBatchCommit(previousBid);
                    }
                }
            }
            else
            {
                Debug.Assert(highestCommittedBid == previousBid);
            }

            this.logger.LogInformation("Finished waiting for previous batch: {prevBid} to finish. Current bid: {bid}", this.GrainReference, previousBid, bid);

            if (this.bidToLastCoordID.ContainsKey(bid)) this.bidToLastCoordID.Remove(bid);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="bid"></param>
        public void AckBatchCommit(long bid)
        {
            this.highestCommittedBid = Math.Max(bid, highestCommittedBid);

            if (this.batchCommit.ContainsKey(bid))
            {
                this.logger.LogInformation("Batch: {bid} can now commit", this.GrainReference, bid);
                this.batchCommit[bid].SetResult(true);
                this.batchCommit.Remove(bid);
            }
        }

        public void GarbageCollectTokenInfo(TokenBase token)
        {
            Debug.Assert(!this.isRegionalCoordinator);
            var expiredGrains = new HashSet<Tuple<int, string>>();

            // only when last batch is already committed, the next emitted batch can have its lastBid = -1 again
            foreach (var item in token.previousBidPerService)
            {
                if (item.Value <= highestCommittedBid)
                {
                     expiredGrains.Add(item.Key);
                }
            }

            foreach (var item in expiredGrains)
            {
                token.previousBidPerService.Remove(item);
                token.previousRegionalBidPerGrain.Remove(item);
            }

            token.HighestCommittedBid = highestCommittedBid;
        }

        #endregion
    }
}
