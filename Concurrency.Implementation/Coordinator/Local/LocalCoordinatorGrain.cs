using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator.Models;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Utilities;

namespace Concurrency.Implementation.Coordinator.Local
{
    [Reentrant]
    [LocalCoordinatorGrainPlacementStrategy]
    public class LocalCoordinatorGrain : Grain, ILocalCoordinatorGrain
    {
        private string region;
        private Random random;
        private long myId;
        private long highestCommittedBid;

        // coord basic info
        private ILocalCoordinatorGrain neighborCoord;
        private readonly ILogger logger;

        // PACT
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<GrainAccessInfo, SubBatch>> bidToSubBatches;

        // Hierarchical Architecture
        // for global batches sent from global coordinators
        private SortedDictionary<long, SubBatch> regionalBatchInfo;                                   // key: regional bid
        private Dictionary<long, Dictionary<long, List<GrainAccessInfo>>> regionalTransactionInfo; // <regional bid, <regional tid, grainAccessInfo>>
        private Dictionary<long, TaskCompletionSource<Tuple<long, long>>> regionalDetRequestPromise;  // <regional tid, <local bid, local tid>>
        private Dictionary<long, long> localBidToRegionalBid;
        private Dictionary<long, Dictionary<long, long>> regionalTidToLocalTidPerBatch;               // local bid, <regional tid, local tid>

        // for regional batch commitment
        private long highestCommittedRegionalBid;
        private Dictionary<long, long> regionalBidToRegionalCoordID;
        private Dictionary<long, bool> regionalBidToIsPreviousBatchRegional;                                // regional bid, if this batch's previous one is also a regional batch
        private Dictionary<long, TaskCompletionSource<bool>> regionalBatchCommit;                     // regional bid, commit promise



        // transaction processing
        private List<List<GrainAccessInfo>> deterministicRequests;
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

            this.deterministicRequests = new List<List<GrainAccessInfo>>();
            this.deterministicRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<GrainAccessInfo, SubBatch>>();
            this.bidToLastBid = new Dictionary<long, long>();
            this.bidToLastCoordID = new Dictionary<long, long>(); // <bid, coordID who emit this bid's lastBid>

            this.regionalBatchInfo = new SortedDictionary<long, SubBatch>();
            this.regionalTransactionInfo = new Dictionary<long, Dictionary<long, List<GrainAccessInfo>>>();
            this.regionalDetRequestPromise = new Dictionary<long, TaskCompletionSource<Tuple<long, long>>>();
            this.localBidToRegionalBid = new Dictionary<long, long>();
            this.regionalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            this.regionalBidToIsPreviousBatchRegional = new Dictionary<long, bool>();
            this.regionalBatchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.regionalBidToRegionalCoordID = new Dictionary<long, long>();
        }


        // for PACT
        public async Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos)
        {
            this.logger.LogInformation("NewLocalTransaction is called with grainAccessInfo: {grainAccessInfo}, grainClassNames: {grainClassNames}",
                                       this.GrainReference, string.Join(", ", grainAccessInfos));

            Task<Tuple<long, long>> getBidAndTidTask = this.GetDeterministicTransactionBidAndTid(grainAccessInfos);

            var bidAndTid = await getBidAndTidTask;
            long bid = bidAndTid.Item1;
            long tid = bidAndTid.Item2;

            this.logger.LogInformation("NewLocalTransaction is going to return bid: {bid} and tid: {tid}", this.GrainReference, bid, tid);

            return new TransactionRegisterInfo(bid, tid, this.highestCommittedBid);
        }

        public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfos)
        {
            this.logger.LogInformation("NewRegionalTransaction is called regionalBid {globalBid} and regionalTid {tid}", this.GrainReference, regionalBid, regionalTid);

            if (!this.regionalTransactionInfo.ContainsKey(regionalBid))
            {
                this.regionalTransactionInfo.Add(regionalBid, new Dictionary<long, List<GrainAccessInfo>>());
            }

            this.regionalTransactionInfo[regionalBid].Add(regionalTid, grainAccessInfos);

            var promise = new TaskCompletionSource<Tuple<long, long>>();
            this.logger.LogInformation("Waiting for promise {promise} to bet set to a value", this.GrainReference, promise);
            this.regionalDetRequestPromise.Add(regionalTid, promise);
            await promise.Task;

            this.logger.LogInformation("Done waiting for promise {promise} to bet set to a value", this.GrainReference, promise);

            return new TransactionRegisterInfo(promise.Task.Result.Item1, promise.Task.Result.Item2, highestCommittedBid);
        }

        public async Task<Tuple<long, long>> GetDeterministicTransactionBidAndTid(List<GrainAccessInfo> grainAccessInfos)   // returns a Tuple<bid, tid>
        {
            this.deterministicRequests.Add(grainAccessInfos);
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
            curBatchIDs = this.GenerateRegionalBatch(token);
            curBatchID = this.GenerateBatch(token);

            if (this.highestCommittedBid > token.HighestCommittedBid)
            {
                this.GarbageCollectTokenInfo(token);
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
        public async Task BatchCompletionAcknowledgement(long bid)
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
                    await this.RegionalBatchCommitAcknowledgement(regionalBid);

                }
            }

            await this.WaitPrevBatchToCommit(bid);

            if (isRegional)
            {
                if (!isPreviousRegional)
                {
                    await this.RegionalBatchCommitAcknowledgement(regionalBid);
                }

                await this.WaitForRegionalBatchToCommit(regionalBid);

                this.localBidToRegionalBid.Remove(bid);
                this.regionalBidToRegionalCoordID.Remove(regionalBid);
                this.regionalBidToIsPreviousBatchRegional.Remove(regionalBid);
            }

            this.AckBatchCommit(bid);

            Dictionary<GrainAccessInfo, SubBatch> currentScheduleMap = bidToSubBatches[bid];

            // Sent message that the transaction grains can commit
            foreach ((GrainAccessInfo grainId, SubBatch subBatch) in currentScheduleMap)
            {
                this.GetPrimaryKeyLong(out string region);
                this.logger.LogInformation($"Commit Grains", this.GrainReference);
                Debug.Assert(region == grainId.Region); // I think this should be true, we just have the same info multiple places now
                var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Id, region, grainId.GrainClassName);
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
        public async Task WaitForBatchToCommit(long bid)
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


        // Going to be called by our Regional Coordinator
        public Task ReceiveBatchSchedule(SubBatch batch)
        {
            this.logger.LogInformation("Received batch schedule from regional coordinator {regionalCoordinatorId} with previous bid {previousBatchId} and current bid {bid}",
                                       this.GrainReference, batch.LocalCoordinatorId, batch.PreviousBid, batch.Bid);

            var regionalBid = batch.Bid;
            this.regionalBatchInfo.Add(regionalBid, batch);
            this.regionalBidToRegionalCoordID.Add(regionalBid, batch.LocalCoordinatorId);

            if (!this.regionalTransactionInfo.ContainsKey(regionalBid))
            {
                this.regionalTransactionInfo.Add(regionalBid, new Dictionary<long, List<GrainAccessInfo>>());
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
                                       this.GrainReference, regionalBid);

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

                if (subBatch.PreviousBid != token.PreviousEmitRegionalBid)
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

                this.regionalBidToIsPreviousBatchRegional.Add(regionalBid, token.IsLastEmitBidRegional);
                this.regionalBatchInfo.Remove(regionalBid);
                this.regionalTransactionInfo.Remove(regionalBid);

                this.UpdateToken(token, currentBatchId, regionalBid);
                token.PreviousEmitRegionalBid = regionalBid;
            }

            return currentBatchIds;
        }

        public Task RegionalBatchCommitAcknowledgement(long regionalBid)
        {
            this.GetPrimaryKeyLong(out string region);
            string region1 = new string(region);
            var regionalCoordID = this.regionalBidToRegionalCoordID[regionalBid];
            // Just try to get the regional silo somehow to see if it works
            string regionalCoordinatorRegion = region1.Substring(0, 2);
            this.logger.LogInformation("Complete current regional batch: {regionalBid} to RegionalCoordinator {id}-{region}", this.GrainReference, regionalBid, regionalCoordID, regionalCoordinatorRegion);
            //this.logger.Info($"[{region}] LocalCoordinatorGrain is going to call AckBatchCompletion on the regional coordinator:{regionalCoordinatorRegion} ID:{globalCoordID}");

            var regionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordID, regionalCoordinatorRegion);

            this.logger.LogInformation("Got regional coordinator {id}-{region}", this.GrainReference, regionalCoordID, regionalCoordinatorRegion);

            _ = regionalCoordinator.AckBatchCompletion(regionalBid);
            this.logger.LogInformation("Called AckBachCompletion!!! with bid: {bid}", this.GrainReference, regionalBid);

            return Task.CompletedTask;
        }

        private async Task WaitForRegionalBatchToCommit(long regionalBid)
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

            // Waiting here for the RegionalCoordinator to sent a signal to commit for regionalBid.
            await this.regionalBatchCommit[regionalBid].Task;
        }


        private async Task EmitBatch(long bid)
        {
            Dictionary<GrainAccessInfo, SubBatch> currentScheduleMap = this.bidToSubBatches[bid];

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

            foreach ((GrainAccessInfo grainId, SubBatch subBatch) in currentScheduleMap)
            {
                int id = grainId.Id;
                string region = grainId.Region;

                this.logger.LogInformation("Calling EmitBatch on transaction execution grain: {grainId}", this.GrainReference, grainId);

                // I think this should be true, we just have the same info multiple places now
                // The problem is if this is not true, then the local coordinator is talking to
                // grains in other servers

                var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(id, region, grainId.GrainClassName);

                var localSubBatch = new LocalSubBatch(subBatch)
                {
                    RegionalBid = regionalBid,
                    HighestCommittedBid = this.highestCommittedBid,
                    RegionalTidToLocalTid = regionalTidToLocalTid
                };

                _ = destination.ReceiveBatchSchedule(localSubBatch);
            }
        }


        /// <summary>
        /// This is called every time the corresponding coordinator receives the token.
        /// </summary>
        /// <returns>batchId</returns>
        public long GenerateBatch(LocalToken token)
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
        public void GenerateSchedulePerService(long tid, long currentBatchId, List<GrainAccessInfo> deterministicRequests)
        {
            this.logger.LogInformation("GenerateSchedulePerService", this.GrainReference);
            if (!this.bidToSubBatches.ContainsKey(currentBatchId))
            {
                this.bidToSubBatches.Add(currentBatchId, new Dictionary<GrainAccessInfo, SubBatch>());
            }

            Dictionary<GrainAccessInfo, SubBatch> deterministicRequestToSubBatch = this.bidToSubBatches[currentBatchId];

            for (int i = 0; i < deterministicRequests.Count; i++)
            {
                var grainId = deterministicRequests[i];

                if (!deterministicRequestToSubBatch.ContainsKey(grainId))
                {
                    deterministicRequestToSubBatch.Add(grainId, new SubBatch(currentBatchId, myId));
                }

                // TODO: This seems pretty sketchy. Why do we add the same tid so many times?
                deterministicRequestToSubBatch[grainId].Transactions.Add(tid);
            }
        }

        public void UpdateToken(LocalToken token, long currentBatchId, long globalBid)
        {
            // Here we assume that every actor is only called once
            Dictionary<GrainAccessInfo, SubBatch> serviceIDToSubBatch = this.bidToSubBatches[currentBatchId];
            this.expectedAcksPerBatch.Add(currentBatchId, serviceIDToSubBatch.Count);
            this.logger.LogInformation("UpdateToken: for current batch: {bid} and token: {token}", this.GrainReference, currentBatchId, token);

            // update the previous batch ID for each service accessed by this batch
            foreach (var serviceInfo in serviceIDToSubBatch)
            {
                GrainAccessInfo serviceId = serviceInfo.Key;
                SubBatch subBatch = serviceInfo.Value;
                this.logger.LogInformation("service: {service} and subbatch: {subbatch}", this.GrainReference, serviceId, subBatch);

                if (token.PreviousBidPerGrain.ContainsKey(serviceId))
                {
                    this.logger.LogInformation("New subbatch previousBid value: {value}", this.GrainReference, token.PreviousBidPerGrain[serviceId]);
                    subBatch.PreviousBid = token.PreviousBidPerGrain[serviceId];
                    subBatch.PreviousRegionalBid = token.PreviousRegionalBidPerGrain[serviceId];
                }
                // else, the default value is -1

                Debug.Assert(subBatch.Bid > subBatch.PreviousBid);
                token.PreviousBidPerGrain[serviceId] = subBatch.Bid;
                token.PreviousRegionalBidPerGrain[serviceId] = globalBid;
            }

            this.bidToLastBid.Add(currentBatchId, token.PreviousEmitBid);

            if (token.PreviousEmitBid != -1)
            {
                this.bidToLastCoordID.Add(currentBatchId, token.PreviousCoordinatorId);
            }

            token.PreviousEmitBid = currentBatchId;
            token.IsLastEmitBidRegional = globalBid != -1;
            token.PreviousCoordinatorId = this.myId;

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
                    await this.WaitForBatchToCommit(previousBid);
                }
                else
                {
                    this.logger.LogInformation("FUCKING HERP DERP", this.GrainReference);
                    this.GrainReference.GetPrimaryKeyLong(out string region);
                    var previousBatchCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(coordinator, region);
                    await previousBatchCoordinator.WaitForBatchToCommit(previousBid);
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

        public void GarbageCollectTokenInfo(LocalToken token)
        {
            var expiredGrains = new HashSet<GrainAccessInfo>();

            // only when last batch is already committed, the next emitted batch can have its lastBid = -1 again
            foreach (var item in token.PreviousBidPerGrain)
            {
                if (item.Value <= this.highestCommittedBid)
                {
                    expiredGrains.Add(item.Key);
                }
            }

            foreach (var item in expiredGrains)
            {
                token.PreviousBidPerGrain.Remove(item);
                token.PreviousRegionalBidPerGrain.Remove(item);
            }

            token.HighestCommittedBid = this.highestCommittedBid;
        }
    }
}
//     /// <summary>
//     /// Class for the API of the LocalCoordinator. The LocalCoordinator needs to be able to talk with both the TransactionExecutionGrain and
//     /// the RegionalCoordinator
//     /// </summary>
//     [Reentrant]
//     [LocalCoordinatorGrainPlacementStrategy]
//     public class LocalCoordinatorGrain : Grain, ILocalCoordinatorGrain
//     {
//         private readonly ILogger<LocalCoordinatorGrain> logger;
//         private readonly ILocalDeterministicTransactionProcessorFactory localDeterministicTransactionProcessorFactory;
//         private ILocalCoordinatorGrain neighbor;

//         public LocalCoordinatorGrain(ILogger<LocalCoordinatorGrain> logger,
//                                      ILocalDeterministicTransactionProcessorFactory localDeterministicTransactionProcessorFactory)
//         {
//             this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

//             this.highestCommittedBid = -1;
//             this.highestCommittedRegionalBid = -1;

//             this.expectedAcksPerBatch = new Dictionary<long, int>();
//             this.localBatchProcessInfos = new Dictionary<long, LocalBatchProcessInfo>();

//             this.deterministicTransactionRequests = new List<List<GrainAccessInfo>>();
//             this.deterministicTransactionRequestPromises = new List<TaskCompletionSource<TransactionId>>();

//             this.regionalBatchInfos = new SortedDictionary<long, SubBatch>();
//             this.regionalDeterministicTransactionRequestPromises = new Dictionary<long, TaskCompletionSource<TransactionId>>();
//             this.regionalBatchProcessInfos = new Dictionary<long, RegionalBatchProcessInfo>();
//         }

//         public override Task OnActivateAsync()
//         {

//             return Task.CompletedTask;
//         }

//         #region TransactionExecutionGrain
//         public async Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos)
//         {
//             Task<TransactionRegisterInfo> task = this.NewLocalTransaction1(grainAccessInfos);

//             return await task;
//         }

//         public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfos)
//         {
//             Task<TransactionRegisterInfo> task = this.NewRegionalTransaction1(regionalBid, regionalTid, grainAccessInfos);

//             return await task;
//         }

//         /// <summary>
//         /// This is called from the TransactionExecutionGrain when the grain is done with its transactions in its current subbatch.
//         /// If this is a regional transaction we will then wait for commit confirmation from the corresponding regional coordinator.
//         /// If this is a local transaction we will wait until previous batch is committed.
//         /// Also responsible to sent the commit confirmation to the TransactionExecutionGrains.
//         /// </summary>
//         /// <param name="bid"></param>
//         /// <returns></returns>
//         public async Task BatchCompletionAcknowledgement(long bid)
//         {
//             Task task = this.BatchCompletionAcknowledgement1(bid);

//             await task;
//         }

//         #endregion

//         #region LocalCoordinator
//         public async Task WaitForBatchToCommit(long bid)
//         {
//             Task task = this.WaitForBatchToCommit1(bid);

//             await task;
//         }

//         public async Task PassToken(LocalToken token)
//         {
//             IList<long> currentBatchIds = this.GenerateRegionalBatch(token);
//             long currentBatchId = this.GenerateLocalBatch(token);

//             Thread.Sleep(10);

//             _ = this.neighbor.PassToken(token);
//             if (currentBatchId != -1) await this.EmitBatch(currentBatchId);

//             if (currentBatchIds.Count > 0)
//             {
//                 this.logger.LogInformation("BatchIds: {ids}", this.GrainReference, string.Join(", ", currentBatchIds));
//             }

//             foreach (var bid in currentBatchIds)
//             {
//                 await this.EmitBatch(bid);
//             }
//         }

//         #endregion

//         #region RegionalCoordinator

//         public async Task RegionalBatchCommitAcknowledgement(long regionalBid)
//         {
//             Task task = this.AckRegionalBatchCommit(regionalBid);

//             await task;
//         }

//         public async Task ReceiveBatchSchedule(SubBatch batch)
//         {
//             Task task = this.ReceiveBatchSchedule1(batch);

//             await task;
//         }

//         #endregion

//         public Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor)
//         {
//             this.neighbor = neighbor;

//             return Task.CompletedTask;
//         }

//         private Dictionary<long, int> expectedAcksPerBatch;

//         private readonly Dictionary<long, LocalBatchProcessInfo> localBatchProcessInfos;
//         // private readonly Dictionary<long, RegionalBatchProcessInfo> regionalBatchProcessInfos;
//         private readonly Dictionary<long, RegionalBatchProcessInfo> regionalBatchProcessInfos;

//         private readonly List<List<GrainAccessInfo>> deterministicTransactionRequests;
//         private readonly List<TaskCompletionSource<TransactionId>> deterministicTransactionRequestPromises; // <local bid, local tid>
//         private readonly SortedDictionary<long, SubBatch> regionalBatchInfos;                // key: regional bid
//         private readonly Dictionary<long, TaskCompletionSource<TransactionId>> regionalDeterministicTransactionRequestPromises;  // <regional tid, <local bid, local tid>>

//         private long highestCommittedRegionalBid;
//         private long highestCommittedBid;


//         private async Task<TransactionRegisterInfo> NewLocalTransaction1(List<GrainAccessInfo> grainAccessInfos)
//         {
//             this.logger.LogInformation("NewLocalTransaction is called with grainAccessInfo: [{grainAccessInfo}]",
//                                        this.GrainReference, string.Join(", ", grainAccessInfos));

//             this.deterministicTransactionRequests.Add(grainAccessInfos);
//             var transactionIdPromise = new TaskCompletionSource<TransactionId>();

//             // We are waiting until the token is arrived which then will create the tuple.
//             this.deterministicTransactionRequestPromises.Add(transactionIdPromise);

//             this.logger.LogInformation("Waiting for the token to arrive. Size: {size}", this.GrainReference, this.deterministicTransactionRequests.Count);
//             var transactionId = await transactionIdPromise.Task;

//             long bid = transactionId.Bid;
//             long tid = transactionId.Tid;

//             this.logger.LogInformation("NewLocalTransaction is going to return bid: {bid} and tid: {tid}", this.GrainReference, bid, tid);

//             return new TransactionRegisterInfo(bid, tid, this.highestCommittedBid);
//         }

//         // TODO: We should inject a CoordinatorProvider such that we can choose a regionalCoordinator
//         public async Task<TransactionRegisterInfo> NewRegionalTransaction1(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfo)
//         {
//             this.logger.LogInformation("NewRegionalTransaction is called regionalBid {regionalBid} and regionalTid {tid}", this.GrainReference, regionalBid, regionalTid);

//             if (!this.regionalBatchProcessInfos.TryGetValue(regionalBid, out RegionalBatchProcessInfo regionalBatchProcessInfo))
//             {
//                 this.regionalBatchProcessInfos.Add(regionalBid, regionalBatchProcessInfo = new RegionalBatchProcessInfo());
//             }

//             regionalBatchProcessInfo.RegionalTransactionInfos.TryAdd(regionalTid, grainAccessInfo);

//             var transactionIdPromise = new TaskCompletionSource<TransactionId>();
//             this.logger.LogInformation("Waiting for regional transactionIdPromise {promise} to bet set to a value", this.GrainReference, transactionIdPromise);
//             this.regionalDeterministicTransactionRequestPromises.TryAdd(regionalTid, transactionIdPromise);

//             TransactionId transactionId = await transactionIdPromise.Task;

//             this.logger.LogInformation("Done waiting for promise {promise} to bet set to a value", this.GrainReference, transactionIdPromise);

//             return new TransactionRegisterInfo(transactionId.Bid, transactionId.Tid, highestCommittedBid);
//         }

//         /// <summary>
//         /// This is called every time the corresponding coordinator receives the token.
//         /// </summary>
//         /// <returns>batchId</returns>
//         public long GenerateLocalBatch(LocalToken token)
//         {
//             if (this.deterministicTransactionRequests.Count == 0)
//             {
//                 return -1;
//             }

//             this.logger.LogInformation("GenerateLocalBatch2: {size}", this.GrainReference, this.deterministicTransactionRequests.Count);

//             // assign bid and tid to waited PACTs
//             var currentBatchId = token.PreviousEmitTid + 1;

//             if(!this.localBatchProcessInfos.TryGetValue(currentBatchId, out LocalBatchProcessInfo localBatchProcessInfo))
//             {
//                 this.localBatchProcessInfos.Add(currentBatchId, localBatchProcessInfo = new LocalBatchProcessInfo());
//             }

//             for (int i = 0; i < this.deterministicTransactionRequests.Count; i++)
//             {
//                 var tid = ++token.PreviousEmitTid;
//                 // Be aware that currently it has to be side effects!
//                 this.GenerateSchedulePerGrain(this.localBatchProcessInfos[currentBatchId].SchedulePerGrain, tid, currentBatchId, this.deterministicTransactionRequests[i]);

//                 this.logger.LogInformation("herpderp count: {count}, currentBatchId: {currentBatchId}", this.localBatchProcessInfos[currentBatchId].SchedulePerGrain.Count, currentBatchId);

//                 var transactionId = new TransactionId() { Tid = tid, Bid = currentBatchId };

//                 this.deterministicTransactionRequestPromises[i].SetResult(transactionId);
//                 this.logger.LogInformation("Setting transactionId to a value: {transactionId}", this.GrainReference, transactionId);
//             }

//             this.UpdateToken(token, currentBatchId, -1);

//             this.deterministicTransactionRequests.Clear();
//             this.deterministicTransactionRequestPromises.Clear();
//             this.GarbageCollection(token);

//             return currentBatchId;
//         }

//         public List<long> GenerateRegionalBatch(LocalToken token)
//         {
//             List<long> currentBatchIds = new List<long>();

//             while (this.regionalBatchInfos.Count > 0)
//             {
//                 var bidAndBatch = this.regionalBatchInfos.First();
//                 var regionalBid = bidAndBatch.Key;
//                 var subBatch = bidAndBatch.Value;

//                 if (!this.regionalBatchProcessInfos.TryGetValue(regionalBid, out RegionalBatchProcessInfo regionalBatchProcessInfo))
//                 {
//                     this.logger.LogWarning("RegionalBatchProcessInfo was not in the dictionary when generatingRegionalBatch: {regionalBid}", regionalBid);
//                     this.regionalBatchProcessInfos.Add(regionalBid, regionalBatchProcessInfo = new RegionalBatchProcessInfo());
//                 }

//                 if (subBatch.PreviousBid != token.PreviousEmitRegionalBid)
//                 {
//                     this.logger.LogInformation("previousBid: {bid} - PreviousEmitBid: {emitBid}", this.GrainReference, subBatch.PreviousBid, token.PreviousEmitRegionalBid);
//                     return new List<long>();
//                 }

//                 if (subBatch.Transactions.Count != regionalBatchProcessInfo.RegionalTransactionInfos.Count)
//                 {
//                     this.logger.LogInformation("Herp2", this.GrainReference);
//                     return new List<long>();
//                 }

//                 this.logger.LogInformation("HerpDerp regionalBid: {regionalBid}, subbatch: {batch}", this.GrainReference, regionalBid, subBatch);

//                 var currentBatchId = token.PreviousEmitTid + 1;
//                 this.localBatchProcessInfos.TryAdd(currentBatchId, new LocalBatchProcessInfo());

//                 LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[currentBatchId];
//                 localBatchProcessInfo.RegionalBid = regionalBid;
//                 currentBatchIds.Add(currentBatchId);

//                 foreach (var regionalTid in subBatch.Transactions)
//                 {
//                     this.logger.LogInformation("Succes: previousBid: {bid} - PreviousEmitBid: {emitBid}", this.GrainReference, subBatch.PreviousBid, token.PreviousEmitRegionalBid);

//                     var localTid = ++token.PreviousEmitTid;
//                     TransactionId transactionId = new TransactionId() { Bid = currentBatchId, Tid = localTid };

//                     var transactionIdPromise = new TaskCompletionSource<TransactionId>();
//                     this.regionalDeterministicTransactionRequestPromises.TryAdd(regionalTid, transactionIdPromise);

//                     this.regionalDeterministicTransactionRequestPromises[regionalTid].SetResult(transactionId);

//                     var grainAccessInfo = regionalBatchProcessInfo.RegionalTransactionInfos[regionalTid];

//                     this.GenerateSchedulePerGrain(localBatchProcessInfo.SchedulePerGrain, localTid, currentBatchId, grainAccessInfo);

//                     this.logger.LogInformation("GenerateRegionalBatch: Count: {count} --- schedulePerGrain: [{grain}]", this.GrainReference, localBatchProcessInfo.SchedulePerGrain.Count, string.Join(";; ", localBatchProcessInfo.SchedulePerGrain.Select(kv => kv.Key + ": " + kv.Value)));

//                     localBatchProcessInfo.RegionalToLocalTidMapping.TryAdd(regionalTid, localTid);
//                     this.regionalDeterministicTransactionRequestPromises.Remove(regionalTid);
//                 }

//                 regionalBatchProcessInfo.IsPreviousBatchRegional = token.IsLastEmitBidRegional;

//                 this.regionalBatchInfos.Remove(regionalBid);

//                 this.UpdateToken(token, currentBatchId, regionalBid);

//                 token.PreviousEmitRegionalBid = regionalBid;
//                 this.logger.LogInformation("PreviousEmitRegionalBid: {bid}", this.GrainReference, token.PreviousEmitRegionalBid);
//             }

//             this.GarbageCollection(token);

//             return currentBatchIds;
//         }

//         private void GenerateSchedulePerGrain(IDictionary<GrainAccessInfo, SubBatch> schedulePerGrain, long tid, long currentBatchId,
//                                               List<GrainAccessInfo> deterministicTransactionRequests)
//         {
//             this.logger.LogInformation("GenerateSchedulePerService: Number of transactionRequest: {count}. The grains accessed are: {grains}", this.GrainReference, deterministicTransactionRequests.Count, string.Join(", ", deterministicTransactionRequests));

//             for (int i = 0; i < deterministicTransactionRequests.Count; i++)
//             {
//                 var grainId = deterministicTransactionRequests[i];

//                 if (!schedulePerGrain.ContainsKey(grainId))
//                 {
//                     schedulePerGrain.Add(grainId, new SubBatch(currentBatchId, this.GrainReference.GetPrimaryKeyLong(out _)));
//                 }

//                 schedulePerGrain[grainId].Transactions.Add(tid);
//             }

//             this.logger.LogInformation("GenerateSchedulePerService Done", this.GrainReference);
//         }

//         public Task EmitBatch(long bid)
//         {
//             IDictionary<GrainAccessInfo, SubBatch> currentScheduleMap = this.localBatchProcessInfos[bid].SchedulePerGrain;
//             this.logger.LogInformation("localbatchprocessinfos: {infos}, bid: {bid}", this.GrainReference, string.Join(", ", currentScheduleMap.Select(kv => kv.Key + " : " + kv.Value)), bid);

//             long regionalBid = this.localBatchProcessInfos.ContainsKey(bid) ? this.localBatchProcessInfos[bid].RegionalBid : -1;
//             this.logger.LogInformation("Contains: {c}, regionalBid: {regionalBid}, value: {value}, count: {count}", this.GrainReference, this.localBatchProcessInfos.ContainsKey(bid), regionalBid, this.localBatchProcessInfos[bid].RegionalBid, currentScheduleMap.Count);

//             var regionalTidToLocalTid = new Dictionary<long, long>();

//             if (this.localBatchProcessInfos.ContainsKey(bid))
//             {
//                 regionalTidToLocalTid = this.localBatchProcessInfos[bid].RegionalToLocalTidMapping;
//             }

//             foreach ((var grainAccessInfo , SubBatch subBatch) in currentScheduleMap)
//             {
//                 int id = grainAccessInfo.Id;
//                 string region = grainAccessInfo.Region;

//                 this.logger.LogInformation("Calling EmitBatch on transaction execution grain {grain} with regionalbid: {regionalBid} ", this.GrainReference, grainAccessInfo, regionalBid);

//                 // I think this should be true, we just have the same info multiple places now
//                 // The problem is if this is not true, then the local coordinator is talking to
//                 // grains in other servers

//                 var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(id, region, grainAccessInfo.GrainClassName);

//                 var localSubBatch = new LocalSubBatch(subBatch)
//                 {
//                     RegionalBid = regionalBid,
//                     HighestCommittedBid = this.highestCommittedBid,
//                     RegionalTidToLocalTid = regionalTidToLocalTid
//                 };

//                 _ = destination.ReceiveBatchSchedule(localSubBatch);
//             }

//             return Task.CompletedTask;
//         }

//         public void UpdateToken(LocalToken token, long currentBatchId, long regionalBid)
//         {
//             // Here we assume that every actor is only called once
//             if(!this.localBatchProcessInfos.TryGetValue(currentBatchId, out LocalBatchProcessInfo localBatchProcessInfo))
//             {
//                 this.localBatchProcessInfos.Add(currentBatchId, localBatchProcessInfo = new LocalBatchProcessInfo());
//             }

//             IDictionary<GrainAccessInfo, SubBatch> grainIdToSubBatch = localBatchProcessInfo.SchedulePerGrain;

//             this.logger.LogInformation("UpdateToken: Count: {count} --- grainIdToSubBatch {dict}", this.GrainReference, string.Join(", ", grainIdToSubBatch.Select(kv => kv.Key + ": " + kv.Value)), grainIdToSubBatch.Count);
//             this.expectedAcksPerBatch.TryAdd(currentBatchId, grainIdToSubBatch.Count);
//             this.logger.LogInformation("UpdateToken: for current batch: {bid} and token: {token}", this.GrainReference, currentBatchId, token);

//             // update the previous batch ID for each service accessed by this batch
//             foreach ((GrainAccessInfo grainAccessInfo, SubBatch subBatch) in grainIdToSubBatch)
//             {
//                 this.logger.LogInformation("Grain: {grainId} and subbatch: {subbatch}", this.GrainReference, grainAccessInfo, subBatch);
//                 this.logger.LogInformation("CurrentGrain: {grain} --- PreviuosBidPerGrain: {herp}",
//                                             this.GrainReference, grainAccessInfo, token.PreviousBidPerGrain.Select(kv => kv.Key + ":: " + kv.Value));

//                 if (token.PreviousBidPerGrain.ContainsKey(grainAccessInfo))
//                 {
//                     this.logger.LogInformation("New subbatch previousBid value: {value}", this.GrainReference, token.PreviousBidPerGrain[grainAccessInfo]);
//                     subBatch.PreviousBid = token.PreviousBidPerGrain[grainAccessInfo];
//                     subBatch.PreviousRegionalBid = token.PreviousRegionalBidPerGrain[grainAccessInfo];
//                 }

//                 this.logger.LogInformation("New subbatch: {subbatch}", this.GrainReference, subBatch);
//                 // else, the default value is -1

//                 Debug.Assert(subBatch.Bid > subBatch.PreviousBid);
//                 token.PreviousBidPerGrain[grainAccessInfo] = subBatch.Bid;
//                 token.PreviousRegionalBidPerGrain[grainAccessInfo] = regionalBid;
//             }

//             localBatchProcessInfo.PreviousBid = token.PreviousEmitBid;

//             if (token.PreviousEmitBid != -1)
//             {
//                 localBatchProcessInfo.PreviousCoordinatorId = token.PreviousCoordinatorId;
//             }

//             token.PreviousEmitBid = currentBatchId;
//             token.IsLastEmitBidRegional = regionalBid != -1;
//             token.PreviousCoordinatorId = this.GrainReference.GetPrimaryKeyLong(out string _);

//             this.logger.LogInformation("updated token: {token}", this.GrainReference, token);
//         }

//         public async Task WaitForPreviousBatchToCommit(long bid)
//         {
//             var localBatchProcessInfo = this.localBatchProcessInfos[bid];
//             var previousBid = localBatchProcessInfo.PreviousBid;
//             this.logger.LogInformation("Waiting for previous batch: {prevBid} to commit. Current bid: {bid}", this.GrainReference, previousBid, bid);

//             if (this.highestCommittedBid < previousBid)
//             {
//                 var previousCoordinatorId = localBatchProcessInfo.PreviousCoordinatorId;
//                 var currentCoordinatorId = this.GrainReference.GetPrimaryKeyLong(out string region);

//                 if (previousCoordinatorId == currentCoordinatorId)
//                 {
//                     await this.WaitForBatchToCommit(previousBid);
//                 }
//                 else
//                 {
//                     this.logger.LogInformation("Current bid {bid} is waiting for the previous bid: {prev} to be done in the other local coordinator: {id}-{region}",
//                                                this.GrainReference, bid, previousBid, previousCoordinatorId, region);

//                     var previousBatchCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(previousCoordinatorId, region);
//                     await previousBatchCoordinator.WaitForBatchToCommit(previousBid);

//                     this.logger.LogInformation("Finished waiting for current bid {bid} to wait for the previous bid: {prev} to be done in the other local coordinator: {id}-{region}",
//                                                this.GrainReference, bid, previousBid, previousCoordinatorId, region);
//                 }
//             }
//             else
//             {
//                 Debug.Assert(this.highestCommittedBid == previousBid);
//             }

//             this.logger.LogInformation("Finished waiting for previous batch: {prevBid} to finish. Current bid: {bid}", this.GrainReference, previousBid, bid);
//         }

//         public async Task WaitForBatchToCommit1(long bid)
//         {
//             LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[bid];

//             if (this.highestCommittedBid == bid) return;

//             if (localBatchProcessInfo.BatchCommitPromise is null)
//             {
//                 localBatchProcessInfo.BatchCommitPromise = new TaskCompletionSource<bool>();
//             }

//             this.logger.LogInformation("Waiting for batch: {bid} to commit", this.GrainReference, bid);

//             await localBatchProcessInfo.BatchCommitPromise.Task;

//             this.logger.LogInformation("Finish waiting for batch: {bid} to commit", this.GrainReference, bid);
//         }

//         public Task BatchCommitAcknowledgement(long bid)
//         {
//             LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[bid];

//             this.highestCommittedBid = Math.Max(bid, this.highestCommittedBid);

//             if (localBatchProcessInfo.BatchCommitPromise is not null)
//             {
//                 this.logger.LogInformation("Batch: {bid} can now commit", this.GrainReference, bid);

//                 localBatchProcessInfo.BatchCommitPromise.SetResult(true);
//             }

//             return Task.CompletedTask;
//         }

//         /// <summary>
//         /// This is called from the TransactionExecutionGrain when the grain is done with its transactions in its current subbatch.
//         /// If this is a regional transaction we will then wait for commit confirmation from the corresponding regional coordinator.
//         /// If this is a local transaction we will wait until previous batch is committed.
//         /// Also responsible to sent the commit confirmation to the TransactionExecutionGrains.
//         /// </summary>
//         /// <param name="bid"></param>
//         /// <returns></returns>
//         public async Task BatchCompletionAcknowledgement1(long bid)
//         {
//             this.logger.LogInformation("Expected acknowledgements for batch: {bid} before decrement: {expectedAcksPerBatch}", this.GrainReference, bid, this.expectedAcksPerBatch[bid]);
//             this.expectedAcksPerBatch[bid]--;

//             if (this.expectedAcksPerBatch[bid] > 0) return;


//             // the batch has been completed in this silo
//             long regionalBid = -1;

//             // TODO: Why does it matter why the previous is a regional or local batch?
//             bool isPreviousRegional = false;
//             LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[bid];
//             bool isRegional = localBatchProcessInfo.IsRegional;

//             this.logger.LogInformation("Got all acknowledgements for batch: {bid}. Is the batch regional: {isRegional} and is the previous batch regional: {isPreviousRegional}",
//                                         this.GrainReference, bid, isRegional, isPreviousRegional);

//             // Some sick optimization.......
//             if (isRegional)
//             {
//                 // ACK the regional coordinator
//                 regionalBid = localBatchProcessInfo.RegionalBid;
//                 RegionalBatchProcessInfo regionalBatchProcessInfo = this.regionalBatchProcessInfos[regionalBid];
//                 isPreviousRegional = regionalBatchProcessInfo.IsPreviousBatchRegional;

//                 if (isPreviousRegional)
//                 {
//                     this.AckCompletionToRegionalCoordinator(regionalBid);

//                 }
//             }

//             await this.WaitForPreviousBatchToCommit(bid);

//             if (isRegional)
//             {
//                 if (!isPreviousRegional)
//                 {
//                     this.AckCompletionToRegionalCoordinator(regionalBid);
//                 }

//                 await this.WaitForRegionalBatchToCommit(regionalBid);
//             }

//             // TODO: Check if this is correct
//             await this.BatchCommitAcknowledgement(bid);

//             IDictionary<GrainAccessInfo, SubBatch> currentScheduleMap = localBatchProcessInfo.SchedulePerGrain;

//             // Sent message that the transaction grains can commit
//             foreach ((GrainAccessInfo grainId, SubBatch subBatch) in currentScheduleMap)
//             {
//                 this.GrainReference.GetPrimaryKeyLong(out string region);
//                 this.logger.LogInformation($"Commit Grains", this.GrainReference);
//                 Debug.Assert(region == grainId.Region); // I think this should be true, we just have the same info multiple places now

//                 var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Id, region, grainId.GrainClassName);
//                 _ = destination.AckBatchCommit(bid);
//             }

//             this.expectedAcksPerBatch.Remove(bid);

//             // TODO: Find a way to cleanup the BatchProcessInfo that we do not use anymore
//             // this.localBatchProcessInfos.Remove(bid);
//             // if(regionalBid != -1) this.regionalBatchProcessInfos.Remove(regionalBid);
//         }

//         private void AckCompletionToRegionalCoordinator(long regionalBid)
//         {
//             this.GrainReference.GetPrimaryKeyLong(out string region);
//             string region1 = new string(region);
//             var regionalCoordID = this.regionalBatchProcessInfos[regionalBid].RegionalCoordinatorId;
//             // Just try to get the regional silo somehow to see if it works
//             string regionalCoordinatorRegion = region1.Substring(0, 2);
//             this.logger.LogInformation("Complete current regional batch: {regionalBid} to RegionalCoordinator {id}-{region}", this.GrainReference, regionalBid, regionalCoordID, regionalCoordinatorRegion);
//             //this.logger.Info($"[{region}] LocalCoordinatorGrain is going to call AckBatchCompletion on the regional coordinator:{regionalCoordinatorRegion} ID:{globalCoordID}");
//             var regionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordID, regionalCoordinatorRegion);

//             _ = regionalCoordinator.AckBatchCompletion(regionalBid);
//         }

//         private async Task WaitForRegionalBatchToCommit(long regionalBid)
//         {
//             if (this.highestCommittedRegionalBid >= regionalBid)
//             {
//                 return;
//             }

//             this.logger.LogInformation("Waiting for the regional batch: {bid} to commit",
//                                         this.GrainReference, regionalBid);

//             // Waiting here for the RegionalCoordinator to sent a signal to commit for regionalBid.
//             await this.regionalBatchProcessInfos[regionalBid].BatchCommitPromise.Task;
//         }

//         /// <summary>
//         /// This is called from the <see cref="RegionalCoordinatorGrain"/> to notify that the current batch id is
//         /// ready for commit.
//         /// </summary>
//         /// <param name="regionalBid">bid to commit</param>
//         /// <returns></returns>
//         public Task AckRegionalBatchCommit(long regionalBid)
//         {
//             this.logger.LogInformation("AckRegionalBatch commit was called from regional coordinator. We can now commit batch: {regionalBid}",
//                                        this.GrainReference,  regionalBid);

//             this.highestCommittedRegionalBid = Math.Max(regionalBid, this.highestCommittedRegionalBid);

//             RegionalBatchProcessInfo regionalBatchProcessInfo = this.regionalBatchProcessInfos[regionalBid];

//             // TODO: Check this is correct
//             regionalBatchProcessInfo.BatchCommitPromise.SetResult(true);

//             return Task.CompletedTask;
//         }

//         public Task ReceiveBatchSchedule1(SubBatch batch)
//         {
//             this.logger.LogInformation("Received batch: {batch} schedule from regional coordinator",
//                                        this.GrainReference, batch);

//             var regionalBid = batch.Bid;

//             if(!this.regionalBatchProcessInfos.TryGetValue(regionalBid, out RegionalBatchProcessInfo regionalBatchProcessInfo))
//             {
//                 this.regionalBatchProcessInfos.Add(regionalBid, new RegionalBatchProcessInfo());
//             }

//             this.logger.LogInformation("RegionalBatchProcessInfo: {info}",
//                                         this.GrainReference, string.Join(", ", this.regionalBatchProcessInfos.Select(kv => kv.Key + ": " + kv.Value)));

//             regionalBatchProcessInfo.RegionalSubBatch = batch;
//             regionalBatchProcessInfo.RegionalCoordinatorId = batch.LocalCoordinatorId;

//             this.regionalBatchInfos.TryAdd(regionalBid, batch);

//             this.logger.LogInformation("All values sat in ReceiveBatchSchedule. The regionalBatchInfos contains {info}", 
//                                        this.GrainReference, string.Join(", ", this.regionalBatchInfos));

//             return Task.CompletedTask;
//         }

//         public void GarbageCollectTokenInfo(LocalToken token)
//         {
//             var expiredGrains = new HashSet<GrainAccessInfo>();

//             // only when last batch is already committed, the next emitted batch can have its lastBid = -1 again
//             foreach (var item in token.PreviousBidPerGrain)
//             {
//                 if (item.Value <= highestCommittedBid)
//                 {
//                      expiredGrains.Add(item.Key);
//                 }
//             }

//             foreach (var item in expiredGrains)
//             {
//                 token.PreviousBidPerGrain.Remove(item);
//                 token.PreviousRegionalBidPerGrain.Remove(item);
//             }

//             token.HighestCommittedBid = this.highestCommittedBid;
//         }

//         private void GarbageCollection(LocalToken token)
//         {
//             if (this.highestCommittedBid > token.HighestCommittedBid)
//             {
//                 GarbageCollectTokenInfo(token);
//             }
//             else
//             {
//                 this.highestCommittedBid = token.HighestCommittedBid;
//             }
//         }

//     }
// }

//         // private readonly ILogger<LocalCoordinatorGrain> logger;
//         // private readonly ILocalDeterministicTransactionProcessorFactory localDeterministicTransactionProcessorFactory;
//         // private ILocalDeterministicTransactionProcessor localDeterministicTransactionProcessor;
//         // private ILocalCoordinatorGrain neighbor;

//         // public LocalCoordinatorGrain(ILogger<LocalCoordinatorGrain> logger,
//         //                              ILocalDeterministicTransactionProcessorFactory localDeterministicTransactionProcessorFactory)
//         // {
//         //     this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
//         //     thisFactory = localDeterministicTransactionProcessorFactory ?? throw new ArgumentNullException(nameof(localDeterministicTransactionProcessorFactory));
//         // }

//         // public override Task OnActivateAsync()
//         // {
//         //     this = thisFactory.Create(this.GrainFactory, this.GrainReference);

//         //     return Task.CompletedTask;
//         // }

//         // #region TransactionExecutionGrain
//         // public async Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos)
//         // {
//         //     Task<TransactionRegisterInfo> task = this.NewLocalTransaction(grainAccessInfos);

//         //     return await task;
//         // }

//         // public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfos)
//         // {
//         //     Task<TransactionRegisterInfo> task = this.NewRegionalTransaction(regionalBid, regionalTid, grainAccessInfos);

//         //     return await task;
//         // }

//         // /// <summary>
//         // /// This is called from the TransactionExecutionGrain when the grain is done with its transactions in its current subbatch.
//         // /// If this is a regional transaction we will then wait for commit confirmation from the corresponding regional coordinator.
//         // /// If this is a local transaction we will wait until previous batch is committed.
//         // /// Also responsible to sent the commit confirmation to the TransactionExecutionGrains.
//         // /// </summary>
//         // /// <param name="bid"></param>
//         // /// <returns></returns>
//         // public async Task BatchCompletionAcknowledgement(long bid)
//         // {
//         //     Task task = this.BatchCompletionAcknowledgement(bid);

//         //     await task;
//         // }

//         // #endregion

//         // #region LocalCoordinator
//         // public async Task WaitForBatchToCommit(long bid)
//         // {
//         //     Task task = this.WaitForBatchToCommit(bid);

//         //     await task;
//         // }

//         // public async Task PassToken(LocalToken token)
//         // {
//         //     IList<long> currentBatchIds = this.GenerateRegionalBatch(token);
//         //     long currentBatchId = this.GenerateLocalBatch(token);

//         //     Thread.Sleep(10);

//         //     _ = this.neighbor.PassToken(token);
//         //     if (currentBatchId != -1) await this.EmitBatch(currentBatchId);

//         //     if (currentBatchIds.Count > 0)
//         //     {
//         //         this.logger.LogInformation("BatchIds: {ids}", this.GrainReference, string.Join(", ", currentBatchIds));
//         //     }

//         //     foreach (var bid in currentBatchIds)
//         //     {
//         //         await this.EmitBatch(bid);
//         //     }
//         // }

//         // #endregion

//         // #region RegionalCoordinator

//         // public async Task RegionalBatchCommitAcknowledgement(long regionalBid)
//         // {
//         //     Task task = this.AckRegionalBatchCommit(regionalBid);

//         //     await task;
//         // }

//         // public async Task ReceiveBatchSchedule(SubBatch batch)
//         // {
//         //     Task task = this.ReceiveBatchSchedule(batch);

//         //     await task;
//         // }

//         // #endregion

//         // public Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor)
//         // {
//         //     this.neighbor = neighbor;

//         //     return Task.CompletedTask;
        // }