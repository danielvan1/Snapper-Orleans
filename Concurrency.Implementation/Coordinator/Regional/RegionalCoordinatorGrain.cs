using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionBroadcasting;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Utilities;

namespace Concurrency.Implementation.Coordinator.Regional
{
    // TODO: Move everything out.
    [Reentrant]
    [RegionalCoordinatorGrainPlacementStrategy]
    public class RegionalCoordinatorGrain : Grain, IRegionalCoordinatorGrain
    {
        private Random random;
        private long myId;
        public long highestCommittedBid;

        // transaction processing
        private IList<List<string>> deterministicRequests;
        private List<TaskCompletionSource<Tuple<long, long>>> deterministicRequestPromise; // <local bid, local tid>

        // batch processing
        private Dictionary<long, long> bidToLastBid;
        private Dictionary<long, long> bidToPreviousCoordinatorId; // <bid, coordID who emit this bid's lastBid>
        private Dictionary<long, TaskCompletionSource<bool>> batchCommit;


        private IRegionalCoordinatorGrain neighborCoord;
        private readonly ILogger<RegionalCoordinatorGrain> logger;
        private readonly ITransactionBroadCasterFactory transactionBroadCasterFactory;
        private ITransactionBroadCaster transactionBroadCaster;

        // PACT
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<string, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        private Dictionary<long, Dictionary<string, Tuple<int, string>>> localCoordinatorPerSiloPerBatch;        // <global bid, siloID, chosen local Coord ID>

        private DateTime timeOfBatchGeneration;
        private double batchSizeInMSecs;
        private string region;

        public RegionalCoordinatorGrain(ILogger<RegionalCoordinatorGrain> logger, ITransactionBroadCasterFactory transactionBroadCasterFactory)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.transactionBroadCasterFactory = transactionBroadCasterFactory ?? throw new ArgumentNullException(nameof(transactionBroadCasterFactory));
        }

        public override Task OnActivateAsync()
        {
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<string, SubBatch>>();
            this.localCoordinatorPerSiloPerBatch = new Dictionary<long, Dictionary<string, Tuple<int, string>>>();
            this.myId = this.GetPrimaryKeyLong(out string region);
            this.region = region;
            this.random = new Random();
            this.highestCommittedBid = -1;

            this.deterministicRequests = new List<List<string>>();
            this.deterministicRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.bidToPreviousCoordinatorId = new Dictionary<long, long>();
            this.bidToLastBid = new Dictionary<long, long>();

            this.transactionBroadCaster = this.transactionBroadCasterFactory.Create(this.GrainFactory);

            return base.OnActivateAsync();
        }

        // for PACT
        public async Task<Tuple<TransactionRegisterInfo, Dictionary<string, Tuple<int, string>>>> NewRegionalTransaction(List<string> silos)
        {
            this.logger.LogInformation("New Regional transaction received. The silos involved in the trainsaction: [{silos}]",
                                        this.GrainReference, string.Join(", ", silos));

            Tuple<long, long> bidAndTid = await this.GetDeterministicTransactionBidAndTid(silos);
            long bid = bidAndTid.Item1;
            long tid = bidAndTid.Item2;
            Debug.Assert(this.localCoordinatorPerSiloPerBatch.ContainsKey(bid));

            this.logger.LogInformation("Returning transaction registration info with bid {bid} and tid {tid}", this.GrainReference, bid, tid);

            var transactionRegisterInfo = new TransactionRegisterInfo(bid, tid, this.highestCommittedBid);  // bid, tid, highest committed bid

            return new Tuple<TransactionRegisterInfo, Dictionary<string, Tuple<int, string>>>(transactionRegisterInfo, this.localCoordinatorPerSiloPerBatch[bid]);
        }

        public async Task PassToken(RegionalToken token)
        {
            long curBatchId = -1;
            Thread.Sleep(10);

            var elapsedTime = (DateTime.Now - this.timeOfBatchGeneration).TotalMilliseconds;
            if (elapsedTime >= batchSizeInMSecs)
            {
                curBatchId = this.GenerateBatch(token);

                if (curBatchId != -1) this.timeOfBatchGeneration = DateTime.Now;
            }

            if (this.highestCommittedBid > token.HighestCommittedBid)
            {
                token.HighestCommittedBid = this.highestCommittedBid;
            }
            else
            {
                this.highestCommittedBid = token.HighestCommittedBid;
            }

            _ = this.neighborCoord.PassToken(token);

            if (curBatchId != -1) await EmitBatch(curBatchId);
        }

        private async Task EmitBatch(long bid)
        {
            this.logger.LogInformation("Going to emit batch {bid}", this.GrainReference, bid);
            Dictionary<string, SubBatch> currentScheduleMap = this.bidToSubBatches[bid];

            Dictionary<string, Tuple<int, string>> coordinators = this.localCoordinatorPerSiloPerBatch[bid];

            foreach ((string siloId,  SubBatch subBatch) in currentScheduleMap)
            {
                var localCoordID = coordinators[siloId];
                var localCoordinatorID = localCoordID.Item1;
                var localCoordinatorRegionAndServer = localCoordID.Item2;
                this.logger.LogInformation("Emit batch to localCoordinator {localCoordinatorID}-{localCoordinatorRegionAndServer} with sub batch {subbatch}",
                                            this.GrainReference, localCoordinatorID, localCoordinatorRegionAndServer, subBatch);
                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorID, localCoordinatorRegionAndServer);

                _ = dest.ReceiveBatchSchedule(subBatch);
            }

            _ = this.transactionBroadCaster.BroadCastRegionalSchedules(this.region, bid, this.bidToLastBid[bid], currentScheduleMap);
        }

        public async Task AckBatchCompletion(long bid)
        {
            this.logger.LogInformation("Received ack batch completion for bid: {bid}. expectedAcksPerBatch {acks}",
                                        this.GrainReference, bid, this.expectedAcksPerBatch[bid]);
            // count down the number of expected ACKs from different silos
            this.expectedAcksPerBatch[bid]--;

            if (this.expectedAcksPerBatch[bid] != 0)
            {
                return;
            }

            // commit the batch
            await this.WaitPrevBatchToCommit(bid);
            this.AckBatchCommit(bid);

            // send ACKs to local coordinators
            Dictionary<string, SubBatch> currentSchedule = this.bidToSubBatches[bid];
            Dictionary<string, Tuple<int, string>> coordinators = this.localCoordinatorPerSiloPerBatch[bid];

            foreach (var item in currentSchedule)
            {
                var localCoordID = coordinators[item.Key];

                var localCoordinatorID = localCoordID.Item1;
                var localCoordinatorRegionAndServer = localCoordID.Item2;

                this.logger.LogInformation("Sending acknowledgements to local coordinator {localCoordinatorId} that batch: {bid} can commit",
                                           this.GrainReference, localCoordinatorID+localCoordinatorRegionAndServer,bid);

                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorID, localCoordinatorRegionAndServer);
                _ = dest.AckRegionalBatchCommit(bid);
            }

            // garbage collection
            this.bidToSubBatches.Remove(bid);
            this.localCoordinatorPerSiloPerBatch.Remove(bid);
            this.expectedAcksPerBatch.Remove(bid);
        }

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

        public Task SpawnGlobalCoordGrain(IRegionalCoordinatorGrain neighbor)
        {
            // TODO: This seems not to be necessary as it is called in the ctor of detTxnProcessor

            this.neighborCoord = neighbor;

            this.batchSizeInMSecs = Constants.batchSizeInMSecsBasic;
            for (int i = Constants.numSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeed;
            this.timeOfBatchGeneration = DateTime.Now;

            return Task.CompletedTask;
        }


        // for PACT
        public async Task<Tuple<long, long>> GetDeterministicTransactionBidAndTid(List<string> serviceList)   // returns a Tuple<bid, tid>
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
                this.GenerateSchedulePerSilo(tid, currentBatchID, this.deterministicRequests[i]);
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
        public void GenerateSchedulePerSilo(long tid, long currentBatchId, List<string> deterministicRequests)
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
            this.logger.LogInformation("UpdateToken: for current batch: {bid} and token: {token}", this.GrainReference, currentBatchId, token);

            // update the previous batch ID for each service accessed by this batch
            foreach (var serviceInfo in siloIdToSubBatch)
            {
                string siloId = serviceInfo.Key;
                SubBatch subBatch = serviceInfo.Value;
                this.logger.LogInformation("SiloId: {siloId} and subbatch: {subbatch}", this.GrainReference, siloId, subBatch);

                this.logger.LogInformation("Old subbatch previous bid: {oldBid}", this.GrainReference, subBatch.PreviousBid);
                if (token.PreviousBidPerSilo.ContainsKey(siloId))
                {
                    this.logger.LogInformation("New subbatch previousBid value: {value}", this.GrainReference, token.PreviousBidPerSilo[siloId]);
                    subBatch.PreviousBid = token.PreviousBidPerSilo[siloId];
                }

                this.logger.LogInformation("New subbatch previous bid: {oldBid}", this.GrainReference, subBatch.PreviousBid);

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

            this.logger.LogInformation("updated token: {token}", this.GrainReference, token);
        }


        public async Task WaitPrevBatchToCommit(long bid)
        {
            var previousBid = this.bidToLastBid[bid];
            this.logger.LogInformation("Waiting for previous batch: {prevBid} to commit. Current bid: {bid}", this.GrainReference, previousBid, bid);
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
                    this.logger.LogInformation("FUCKING HERP DERP", this.GrainReference);
                    this.GrainReference.GetPrimaryKeyLong(out string region);
                    string regionalCoordinatorRegion = region.Substring(0, 2);
                    var previousBatchRegionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(coordinator, regionalCoordinatorRegion);
                    await previousBatchRegionalCoordinator.WaitBatchCommit(previousBid);
                }
            }
            else
            {
                Debug.Assert(this.highestCommittedBid == previousBid);
            }

            this.logger.LogInformation("Finished waiting for previous batch: {prevBid} to finish. Current bid: {bid}", this.GrainReference, previousBid, bid);

            if (this.bidToPreviousCoordinatorId.ContainsKey(bid)) this.bidToPreviousCoordinatorId.Remove(bid);
        }

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
    }
}