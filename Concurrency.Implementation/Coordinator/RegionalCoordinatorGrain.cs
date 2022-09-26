using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [RegionalCoordinatorGrainPlacementStrategy]
    public class RegionalCoordinatorGrain : Grain, IRegionalCoordinatorGrain
    {
        // coord basic info
        private IRegionalCoordinatorGrain neighborCoord;
        private readonly ILogger<RegionalCoordinatorGrain> logger;

        // PACT
        private DetTxnProcessor detTxnProcessor;
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        private Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch;        // <global bid, siloID, chosen local Coord ID>

        private DateTime timeOfBatchGeneration;
        private double batchSizeInMSecs;

        public RegionalCoordinatorGrain(ILogger<RegionalCoordinatorGrain> logger)
        {
            this.logger = logger;
        }

        public override Task OnActivateAsync()
        {
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>>();
            this.localCoordinatorPerSiloPerBatch = new Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>>();
            this.detTxnProcessor = new DetTxnProcessor(
                this.logger,
                this.GrainReference,
                this.GetPrimaryKeyLong(out string _),
                this.expectedAcksPerBatch,
                this.bidToSubBatches,
                this.GrainFactory,
                this.localCoordinatorPerSiloPerBatch);
            return base.OnActivateAsync();
        }


        // for PACT
        public async Task<Tuple<TransactionRegisterInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>>> NewRegionalTransaction(List<Tuple<int, string>> silos)
        {
            this.logger.LogInformation("New Regional transaction received. The silos involved in the trainsaction: [{silos}]  ",
                                        this.GrainReference, string.Join(", ", silos));

            Tuple<long, long> BidAndTid = await detTxnProcessor.GetDeterministicTransactionBidAndTid(silos);
            long bid = BidAndTid.Item1;
            long tid = BidAndTid.Item2;
            Debug.Assert(this.localCoordinatorPerSiloPerBatch.ContainsKey(bid));

            this.logger.LogInformation("Returning transaction registration info with bid {bid} and tid {tid}", this.GrainReference, bid, tid);

            var transactionRegisterInfo = new TransactionRegisterInfo(bid, tid, detTxnProcessor.highestCommittedBid);  // bid, tid, highest committed bid

            return new Tuple<TransactionRegisterInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>>(transactionRegisterInfo, this.localCoordinatorPerSiloPerBatch[bid]);
        }

        public async Task PassToken(BasicToken token)
        {
            long curBatchId = -1;

            var elapsedTime = (DateTime.Now - this.timeOfBatchGeneration).TotalMilliseconds;
            if (elapsedTime >= batchSizeInMSecs)
            {
                curBatchId = detTxnProcessor.GenerateBatch(token);
                if (curBatchId != -1) this.timeOfBatchGeneration = DateTime.Now;
            }

            if (this.detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
            {
                token.highestCommittedBid = this.detTxnProcessor.highestCommittedBid;
            }
            else
            {
                this.detTxnProcessor.highestCommittedBid = token.highestCommittedBid;
            }

            _ = this.neighborCoord.PassToken(token);

            if (curBatchId != -1) _ = EmitBatch(curBatchId);
        }

        private async Task EmitBatch(long bid)
        {
            this.logger.LogInformation("Going to emit batch {bid}", this.GrainReference, bid);
            Dictionary<Tuple<int, string>, SubBatch> currentScheduleMap = this.bidToSubBatches[bid];

            Dictionary<Tuple<int, string>, Tuple<int, string>> coordinators = this.localCoordinatorPerSiloPerBatch[bid];

            foreach (( var id,  SubBatch subBatch) in currentScheduleMap)
            {
                var localCoordID = coordinators[id];
                var localCoordinatorID = localCoordID.Item1;
                var localCoordinatorRegionAndServer = localCoordID.Item2;
                this.logger.LogInformation("Emit batch to {localCoordinatorRegionAndServer} with localCoordinator: {localCoordinatorID}, {subbatch}",
                                            this.GrainReference, localCoordinatorRegionAndServer, localCoordinatorID, string.Join(", ", subBatch));
                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorID, localCoordinatorRegionAndServer);
                _ = dest.ReceiveBatchSchedule(subBatch);
            }
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
            await this.detTxnProcessor.WaitPrevBatchToCommit(bid);
            this.detTxnProcessor.AckBatchCommit(bid);

            // send ACKs to local coordinators
            Dictionary<Tuple<int, string>, SubBatch> curScheduleMap = this.bidToSubBatches[bid];
            Dictionary<Tuple<int, string>, Tuple<int, string>> coordinators = this.localCoordinatorPerSiloPerBatch[bid];

            foreach (var item in curScheduleMap)
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
            await detTxnProcessor.WaitBatchCommit(bid);
            this.logger.LogInformation("Done waiting for batch: {bid} to commit", this.GrainReference, bid);
        }

        public Task SpawnGlobalCoordGrain(IRegionalCoordinatorGrain neighbor)
        {
            // TODO: This seems not to be necessary as it is called in the ctor of detTxnProcessor
            this.detTxnProcessor.Init();

            this.neighborCoord = neighbor;

            this.batchSizeInMSecs = Constants.batchSizeInMSecsBasic;
            for (int i = Constants.numSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeed;
            this.timeOfBatchGeneration = DateTime.Now;

            return Task.CompletedTask;
        }
    }
}