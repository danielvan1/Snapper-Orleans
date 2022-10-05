﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Utilities;

namespace Concurrency.Implementation.Coordinator.Regional
{
    public class DetTxnProcessor
    {
        private readonly ILogger logger;
        private readonly GrainReference grainReference;
        private readonly Random random;
        private readonly long myId;
        public long highestCommittedBid;

        // transaction processing
        private IList<List<string>> deterministicRequests;
        private List<TaskCompletionSource<Tuple<long, long>>> deterministicRequestPromise; // <local bid, local tid>

        // batch processing
        private Dictionary<long, long> bidToLastBid;
        private Dictionary<long, long> bidToPreviousCoordinatorId; // <bid, coordID who emit this bid's lastBid>
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<string, SubBatch>> bidToSubBatches; // <bid, Service ID, subBatch>
        private readonly IGrainFactory grainFactory;
        private Dictionary<long, TaskCompletionSource<bool>> batchCommit;

        // only for global batch
        private Dictionary<long, Dictionary<string, Tuple<int, string>>> localCoordinatorPerSiloPerBatch; // regional bid, silo ID, chosen local coord ID

        public DetTxnProcessor(
            ILogger logger,
            GrainReference grainReference,
            long myId,
            Dictionary<long, int> expectedAcksPerBatch,
            Dictionary<long, Dictionary<string, SubBatch>> bidToSubBatches,
            IGrainFactory grainFactory,
            Dictionary<long, Dictionary<string, Tuple<int, string>>> localCoordinatorPerSiloPerBatch)
        {
            this.logger = logger;
            this.grainReference = grainReference;
            this.random = new Random();
            this.myId = myId;
            this.bidToLastBid = new Dictionary<long, long>();
            this.bidToPreviousCoordinatorId = new Dictionary<long, long>();
            this.expectedAcksPerBatch = expectedAcksPerBatch;
            this.bidToSubBatches = bidToSubBatches;
            this.grainFactory = grainFactory;

            // TODO: Consider if this following two lines are equivalent
            // to the previous code, I think it is.
            this.localCoordinatorPerSiloPerBatch = localCoordinatorPerSiloPerBatch;

            this.Init();
        }

        public void Init()
        {
            this.highestCommittedBid = -1;
            this.deterministicRequests = new List<List<string>>();
            this.deterministicRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // for PACT
        public async Task<Tuple<long, long>> GetDeterministicTransactionBidAndTid(List<string> serviceList)   // returns a Tuple<bid, tid>
        {
            this.deterministicRequests.Add(serviceList);
            var promise = new TaskCompletionSource<Tuple<long, long>>();

            // We are waiting until the token is arrived which then will create the tuple.
            this.deterministicRequestPromise.Add(promise);

            this.logger.LogInformation("Waiting for the token to arrive", this.grainReference);
            var tuple = await promise.Task;

            long bid = tuple.Item1;
            long tid = tuple.Item2;

            this.logger.LogInformation("Token has arrived and sat the bid: {bid} and tid: {tid} ", this.grainReference, bid, tid);

            return new Tuple<long, long>(bid, tid);
        }

        /// <summary>
        /// This is called every time the corresponding coordinator receives the token.
        /// </summary>
        /// <returns>batchId</returns>
        public long GenerateBatch(RegionalToken token)
        {
            if (this.deterministicRequests.Count == 0)
            {
                return -1;
            }

            // assign bid and tid to waited PACTs
            var currentBatchID = token.PreviousEmitTid + 1;

            for (int i = 0; i < this.deterministicRequests.Count; i++)
            {
                var tid = ++token.PreviousEmitTid;
                this.GenerateSchedulePerService(tid, currentBatchID, this.deterministicRequests[i]);
                var herp = new Tuple<long, long>(currentBatchID, tid);
                this.deterministicRequestPromise[i].SetResult(herp);
                this.logger.LogInformation("setting tuple: {herp} to a value", this.grainReference, herp);
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
        public void GenerateSchedulePerService(long tid, long currentBatchId, List<string> deterministicRequests)
        {
            if (!this.bidToSubBatches.ContainsKey(currentBatchId))
            {
                this.bidToSubBatches.Add(currentBatchId, new Dictionary<string, SubBatch>());

                // Maps: currentbatchID => <region, <local coordinator Tuple(id, region)>>
                this.localCoordinatorPerSiloPerBatch.Add(currentBatchId, new Dictionary<string, Tuple<int, string>>());
            }

            Dictionary<string, SubBatch> siloToSubBatch = this.bidToSubBatches[currentBatchId];

            for (int i = 0; i < deterministicRequests.Count; i++)
            {
                var siloId = deterministicRequests[i];

                if (!siloToSubBatch.ContainsKey(siloId))
                {
                    siloToSubBatch.Add(siloId, new SubBatch(currentBatchId, this.myId));

                    // randomly choose a local coord as the coordinator for this batch on that silo
                    int randomlyChosenLocalCoordinatorID = this.random.Next(Constants.numLocalCoordPerSilo);
                    string localCoordinatorSiloId = siloId;
                    this.localCoordinatorPerSiloPerBatch[currentBatchId].Add(siloId, new Tuple<int, string>(randomlyChosenLocalCoordinatorID, localCoordinatorSiloId));
                }

                // TODO: This seems pretty sketchy. Why do we add the same tid so many times?
                siloToSubBatch[siloId].Transactions.Add(tid);
            }
        }

        public void UpdateToken(RegionalToken token, long currentBatchId, long globalBid)
        {
            Dictionary<string, SubBatch> siloIdToSubBatch = this.bidToSubBatches[currentBatchId];
            this.expectedAcksPerBatch.Add(currentBatchId, siloIdToSubBatch.Count);
            this.logger.LogInformation("UpdateToken: for current batch: {bid} and token: {token}", this.grainReference, currentBatchId, token);


            // update the previous batch ID for each service accessed by this batch
            foreach (var serviceInfo in siloIdToSubBatch)
            {
                string siloId = serviceInfo.Key;
                SubBatch subBatch = serviceInfo.Value;
                this.logger.LogInformation("SiloId: {siloId} and subbatch: {subbatch}", this.grainReference, siloId, subBatch);

                if (token.PreviousBidPerSilo.ContainsKey(siloId))
                {
                    this.logger.LogInformation("New subbatch previousBid value: {value}", this.grainReference, token.PreviousBidPerSilo[siloId]);
                    subBatch.PreviousBid = token.PreviousBidPerSilo[siloId];
                }
                // else, the default value is -1

                Debug.Assert(subBatch.Bid > subBatch.PreviousBid);
                token.PreviousBidPerSilo[siloId] = subBatch.Bid;
            }
            this.bidToLastBid.Add(currentBatchId, token.PreviousEmitBid);

            if (token.PreviousEmitBid != -1)
            {
                this.bidToPreviousCoordinatorId.Add(currentBatchId, token.PreviousCoordinatorId);
            }

            token.PreviousEmitBid = currentBatchId;
            token.IsLastEmitBidRegional = globalBid != -1;
            token.PreviousCoordinatorId = this.myId;

            this.logger.LogInformation("updated token: {token}", this.grainReference, token);
        }


        public async Task WaitPrevBatchToCommit(long bid)
        {
            var previousBid = this.bidToLastBid[bid];
            this.logger.LogInformation("Waiting for previous batch: {prevBid} to commit. Current bid: {bid}", this.grainReference, previousBid, bid);
            this.bidToLastBid.Remove(bid);

            if (this.highestCommittedBid < previousBid)
            {
                var coordinator = this.bidToPreviousCoordinatorId[bid];
                if (coordinator == this.myId)
                {
                    await this.WaitBatchCommit(previousBid);
                }
                else
                {
                    this.logger.LogInformation("FUCKING HERP DERP", this.grainReference);
                    this.grainReference.GetPrimaryKeyLong(out string region);
                    string regionalCoordinatorRegion = region.Substring(0, 2);
                    var previousBatchRegionalCoordinator = this.grainFactory.GetGrain<IRegionalCoordinatorGrain>(coordinator, regionalCoordinatorRegion);
                    await previousBatchRegionalCoordinator.WaitBatchCommit(previousBid);
                }
            }
            else
            {
                Debug.Assert(this.highestCommittedBid == previousBid);
            }

            this.logger.LogInformation("Finished waiting for previous batch: {prevBid} to finish. Current bid: {bid}", this.grainReference, previousBid, bid);

            if (this.bidToPreviousCoordinatorId.ContainsKey(bid)) this.bidToPreviousCoordinatorId.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            if (this.highestCommittedBid == bid) return;

            if (!this.batchCommit.ContainsKey(bid))
            {
                this.batchCommit.Add(bid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("Waiting for batch: {bid} to commit", this.grainReference, bid);

            await this.batchCommit[bid].Task;

            this.logger.LogInformation("Finish waiting for batch: {bid} to commit", this.grainReference, bid);
        }

        public void AckBatchCommit(long bid)
        {
            this.highestCommittedBid = Math.Max(bid, highestCommittedBid);
            if (this.batchCommit.ContainsKey(bid))
            {
                this.logger.LogInformation("Batch: {bid} can now commit", this.grainReference, bid);
                this.batchCommit[bid].SetResult(true);
                this.batchCommit.Remove(bid);
            }
        }
    }
}