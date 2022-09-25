﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Utilities;
using Orleans;

namespace Concurrency.Implementation.Coordinator
{
    public class DetTxnProcessor
    {
        private readonly ILogger logger;
        private readonly GrainReference grainReference;
        private readonly Random random;
        private readonly long myId;
        private readonly bool isRegionalCoordinator;
        public long highestCommittedBid;
        readonly ICoordMap coordMap;

        // transaction processing
        private IList<List<Tuple<int, string>>> deterministicRequests;
        private List<TaskCompletionSource<Tuple<long, long>>> detRequestPromise; // <local bid, local tid>

        // batch processing
        private Dictionary<long, long> bidToLastBid;
        private Dictionary<long, long> bidToLastCoordID; // <bid, coordID who emit this bid's lastBid>
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches; // <bid, Service ID, subBatch>
        private readonly IGrainFactory grainFactory;
        private Dictionary<long, TaskCompletionSource<bool>> batchCommit;

        // only for global batch
        private Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch; // global bid, silo ID, chosen local coord ID

        public DetTxnProcessor(
            ILogger logger,
            GrainReference grainReference,
            long myID,
            Dictionary<long, int> expectedAcksPerBatch,
            Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches,
            IGrainFactory grainFactory,
            Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch = null)
        {
            this.logger = logger;
            this.grainReference = grainReference;
            this.random = new Random();
            this.myId = myID;
            this.coordMap = coordMap;
            bidToLastBid = new Dictionary<long, long>();
            bidToLastCoordID = new Dictionary<long, long>();
            this.expectedAcksPerBatch = expectedAcksPerBatch;
            this.bidToSubBatches = bidToSubBatches;
            this.grainFactory = grainFactory;

            // TODO: Consider if this following two lines are equivalent
            // to the previous code, I think it is.
            this.isRegionalCoordinator = localCoordinatorPerSiloPerBatch != null;
            this.localCoordinatorPerSiloPerBatch = localCoordinatorPerSiloPerBatch;

            this.Init();
        }

        public void Init()
        {
            this.highestCommittedBid = -1;
            this.deterministicRequests = new List<List<Tuple<int, string>>>();
            this.detRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // for PACT
        public async Task<Tuple<long, long>> NewDeterministicTransaction(List<Tuple<int, string>> serviceList)   // returns a Tuple<bid, tid>
        {
            this.deterministicRequests.Add(serviceList);
            var promise = new TaskCompletionSource<Tuple<long, long>>();

            // We are waiting until the token is arrived which then will create the tuple.
            this.detRequestPromise.Add(promise);

            this.logger.LogInformation("Waiting for the token to arrive", this.grainReference);
            var tuple = await promise.Task;
            long bid = tuple.Item1;
            long tid = tuple.Item2;

            this.logger.LogInformation("Token has arrived and sat the bid: {bid} and tid: {tid} ", this.grainReference, bid, tid);

            return new Tuple<long, long>(promise.Task.Result.Item1, promise.Task.Result.Item2);
        }

        /// <summary>
        /// This is called every time the corresponding coordinator receives the token.
        /// </summary>
        /// <returns>batchId</returns>
        public long GenerateBatch(BasicToken token)
        {
            if (this.deterministicRequests.Count == 0)
            {
                return -1;
            }

            // assign bid and tid to waited PACTs
            var currentBatchID = token.lastEmitTid + 1;

            for (int i = 0; i < this.deterministicRequests.Count; i++)
            {
                var tid = ++token.lastEmitTid;
                this.GenerateSchedulePerService(tid, currentBatchID, this.deterministicRequests[i]);
                this.detRequestPromise[i].SetResult(new Tuple<long, long>(currentBatchID, tid));
            }

            this.UpdateToken(token, currentBatchID, -1);

            deterministicRequests.Clear();
            detRequestPromise.Clear();

            return currentBatchID;
        }

        // GenerateSchedulePerService could be called as a:
        //
        // - Regional coordinator (Then 'service' refers to local coordinator)
        // - Local coordinator (Then 'service' refers to execution grains inside silo)
        //
        // This is why we differentiate between the two with isRegionalCoordinator
        public void GenerateSchedulePerService(long tid, long curBatchID, List<Tuple<int, string>> deterministicRequest)
        {
            if (!this.bidToSubBatches.ContainsKey(curBatchID))
            {
                this.bidToSubBatches.Add(curBatchID, new Dictionary<Tuple<int, string>, SubBatch>());
                if (this.isRegionalCoordinator)
                {
                    // Maps: currentbatchID => <region, <local coordinator Tuple(id, region)>>
                    this.localCoordinatorPerSiloPerBatch.Add(curBatchID, new Dictionary<Tuple<int, string>, Tuple<int, string>>());
                }
            }

            Dictionary<Tuple<int, string>, SubBatch> deterministicRequestToSubBatch = this.bidToSubBatches[curBatchID];

            for (int i = 0; i < deterministicRequest.Count; i++)
            {
                var grainId = deterministicRequest[i];

                if (!deterministicRequestToSubBatch.ContainsKey(grainId))
                {
                    deterministicRequestToSubBatch.Add(grainId, new SubBatch(curBatchID, myId));
                    if (this.isRegionalCoordinator)
                    {
                        // randomly choose a local coord as the coordinator for this batch on that silo
                        // Old code:
                        //var chosenCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToRandomCoordID(serviceID);
                        // New code:
                        int randomlyChosenLocalCoordinatorID = this.random.Next(Constants.numLocalCoordPerSilo);
                        string localCoordinatorServer = grainId.Item2;
                        this.localCoordinatorPerSiloPerBatch[curBatchID].Add(grainId, new Tuple<int, string>(randomlyChosenLocalCoordinatorID, localCoordinatorServer));
                    }
                }

                deterministicRequestToSubBatch[grainId].Transactions.Add(tid);
            }
        }

        public void UpdateToken(BasicToken token, long curBatchID, long globalBid)
        {
            Dictionary<Tuple<int, string>, SubBatch> serviceIDToSubBatch = bidToSubBatches[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, serviceIDToSubBatch.Count);

            // update the last batch ID for each service accessed by this batch
            foreach (var serviceInfo in serviceIDToSubBatch)
            {
                var serviceID = serviceInfo.Key;
                var subBatch = serviceInfo.Value;

                if (token.lastBidPerService.ContainsKey(serviceID))
                {
                    subBatch.PreviousBid = token.lastBidPerService[serviceID];
                    // TODO: Consider this: token.lastGlobalBidPerGrain[new Tuple<int, string>(this.myID, serviceID)];
                    // the old code just used token.lastGlobalBidPerGrain[serviceID];
                    if (!this.isRegionalCoordinator)
                    {
                        subBatch.lastGlobalBid = token.lastGlobalBidPerGrain[serviceID];
                    }
                }
                // else, the default value is -1

                Debug.Assert(subBatch.Bid > subBatch.PreviousBid);
                token.lastBidPerService[serviceID] = subBatch.Bid;
                if (!this.isRegionalCoordinator)
                {
                    token.lastGlobalBidPerGrain[serviceID] = globalBid;
                }
            }
            this.bidToLastBid.Add(curBatchID, token.lastEmitBid);

            if (token.lastEmitBid != -1)
            {
                this.bidToLastCoordID.Add(curBatchID, token.lastCoordID);
            }

            token.lastEmitBid = curBatchID;
            token.isLastEmitBidGlobal = globalBid != -1;
            token.lastCoordID = this.myId;
        }


        public async Task WaitPrevBatchToCommit(long bid)
        {
            var lastBid = this.bidToLastBid[bid];
            this.bidToLastBid.Remove(bid);

            if (highestCommittedBid < lastBid)
            {
                var coordinator = this.bidToLastCoordID[bid];
                if (coordinator == this.myId)
                {
                    await WaitBatchCommit(lastBid);
                }
                else
                {
                    this.logger.LogInformation("FUCKING HERP DERP", this.grainReference);
                    if (this.isRegionalCoordinator)
                    {
                        this.grainReference.GetPrimaryKeyLong(out string region);
                        string regionalCoordinatorRegion = region.Substring(0, 2);
                        var previousBatchRegionalCoordinator = this.grainFactory.GetGrain<IRegionalCoordinatorGrain>(coordinator, regionalCoordinatorRegion);
                        await previousBatchRegionalCoordinator.WaitBatchCommit(lastBid);
                    }
                    else // if it is a local coordinator
                    {
                        this.grainReference.GetPrimaryKeyLong(out string region);
                        var previousBatchCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordinator, region);
                        await previousBatchCoordinator.WaitBatchCommit(lastBid);
                    }
                }
            }
            else
            {
                Debug.Assert(highestCommittedBid == lastBid);
            }

            if (this.bidToLastCoordID.ContainsKey(bid)) this.bidToLastCoordID.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            if (this.highestCommittedBid == bid) return;

            if (!this.batchCommit.ContainsKey(bid))
            {
                this.batchCommit.Add(bid, new TaskCompletionSource<bool>());
            }

            await this.batchCommit[bid].Task;
        }

        public void AckBatchCommit(long bid)
        {
            this.highestCommittedBid = Math.Max(bid, highestCommittedBid);
            if (this.batchCommit.ContainsKey(bid))
            {
                this.batchCommit[bid].SetResult(true);
                this.batchCommit.Remove(bid);
            }
        }

        public void GarbageCollectTokenInfo(BasicToken token)
        {
            Debug.Assert(!this.isRegionalCoordinator);
            var expiredGrains = new HashSet<Tuple<int, string>>();

            // only when last batch is already committed, the next emitted batch can have its lastBid = -1 again
            foreach (var item in token.lastBidPerService)
            {
                if (item.Value <= highestCommittedBid)
                {
                     expiredGrains.Add(item.Key);
                }
            }

            foreach (var item in expiredGrains)
            {
                token.lastBidPerService.Remove(item);
                token.lastGlobalBidPerGrain.Remove(item);
            }

            token.highestCommittedBid = highestCommittedBid;
        }
    }
}
