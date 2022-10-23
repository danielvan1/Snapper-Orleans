using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;

namespace Concurrency.Implementation.Coordinator.Replica
{
    [Reentrant]
    [RegionalCoordinatorGrainPlacementStrategy]
    public class RegionalReplicaCoordinator : Grain, IRegionalReplicaCoordinator
    {
        private Dictionary<long, int> expectedAcknowledgementsPerBatch;
        private Dictionary<long, long> bidToPreviousBid;
        private Dictionary<long, ICollection<string>> bidToSiloIds;
        private Dictionary<long, TaskCompletionSource<bool>> batchCommitPromises;

        private long highestCommittedBid;
        private string currentRegion;
        private readonly ILogger<RegionalReplicaCoordinator> logger;

        public RegionalReplicaCoordinator(ILogger<RegionalReplicaCoordinator> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public override Task OnActivateAsync()
        {
            this.GetPrimaryKeyLong(out string regionKey);

            this.currentRegion = regionKey;
            this.highestCommittedBid = -1;

            this.expectedAcknowledgementsPerBatch = new Dictionary<long, int>();
            this.bidToPreviousBid = new Dictionary<long, long>();
            this.batchCommitPromises = new Dictionary<long, TaskCompletionSource<bool>>();
            this.bidToSiloIds = new Dictionary<long, ICollection<string>>();

            return Task.CompletedTask;
        }

        public Task ReceiveRegionalSchedule(long regionalBid, long previousRegionalBid, Dictionary<string, SubBatch> schedule)
        {
            this.logger.LogInformation("Received regional schedules for batch {bid}, previous batch: {previous} and schedules: {schedule} from master",
                                        this.GrainReference, regionalBid, previousRegionalBid, string.Join(", ", schedule.Select(kv => kv.Key + " :: " + kv.Value)));

            if(!this.expectedAcknowledgementsPerBatch.ContainsKey(regionalBid))
            {
                this.expectedAcknowledgementsPerBatch.Add(regionalBid, schedule.Count);
            }

            if(!this.bidToPreviousBid.ContainsKey(regionalBid))
            {
                this.bidToPreviousBid.Add(regionalBid, previousRegionalBid);
            }

            var siloIds = schedule.Keys;

            var siloIdsWithupdatedDeploymentRegions = this.UpdateDeploymentRegionsToCurrentRegions(siloIds);

            if(!this.bidToSiloIds.ContainsKey(regionalBid))
            {
                this.bidToSiloIds.Add(regionalBid, siloIdsWithupdatedDeploymentRegions);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Sent from the TransactionExecutionGrain that the current batch is done locally. Then check if all
        /// </summary>
        /// <param name="regionalBid"></param>
        /// <returns></returns>
        public async Task CommitAcknowledgement(long regionalBid)
        {
            this.logger.LogInformation("Received commit acknowledgment from LocalCoordinator for RegionalBid: {regionalBid}", this.GrainReference, regionalBid);

            this.expectedAcknowledgementsPerBatch[regionalBid]--;

            if(this.expectedAcknowledgementsPerBatch[regionalBid] > 0)
            {
                return;
            }

            // When this is done we can start to commit the current batch
            await this.WaitForPreviousBatchToCommit(regionalBid);

            this.BatchCommitAcknowledgement(regionalBid);

            // TODO: Change coordinator ID to this
            ICollection<string> siloIds = this.bidToSiloIds[regionalBid];

            // Sent message that the transaction grains can commit
            foreach (string siloId in siloIds)
            {
                this.logger.LogInformation("Sending acknowledgement that regional bid can commit to silo {siloId} for regionalbid: {regionalBid}", this.GrainReference, siloId, regionalBid);

                var destination = this.GrainFactory.GetGrain<ILocalReplicaCoordinator>(0, siloId);
                _ = destination.RegionalBatchCommitAcknowledgement(regionalBid);
            }
        }

        private async Task WaitForPreviousBatchToCommit(long bid)
        {
            long previousBid = this.bidToPreviousBid[bid];

            // This is when it is the first schedule.
            if(previousBid == -1) return;

            this.logger.LogInformation("Current batch: {bid}, waiting for previous batch: {previous} to commit", this.GrainReference, bid, previousBid);

            await this.WaitForBatchToCommit(previousBid);

            this.logger.LogInformation("Done waiting for previous batch: {previous} to commit", this.GrainReference, bid, previousBid);
        }

        private async Task WaitForBatchToCommit(long bid)
        {
            if (this.highestCommittedBid == bid) return;

            if (!this.batchCommitPromises.ContainsKey(bid))
            {
                this.batchCommitPromises.Add(bid, new TaskCompletionSource<bool>());
            }

            // this.logger.LogInformation("Waiting for batch: {bid} to commit", this.grainReference, bid);

            await this.batchCommitPromises[bid].Task;

            // this.logger.LogInformation("Finish waiting for batch: {bid} to commit", this.grainReference, bid);
        }

        private void BatchCommitAcknowledgement(long bid)
        {
            this.highestCommittedBid = Math.Max(bid, this.highestCommittedBid);

            if (this.batchCommitPromises.ContainsKey(bid))
            {
                // this.logger.LogInformation("Batch: {bid} can now commit", this.grainReference, bid);
                this.batchCommitPromises[bid].SetResult(true);
                this.batchCommitPromises.Remove(bid);
            }
        }

        /// <summary>
        /// Updates the given siloids deployment region to the current region.
        /// </summary>
        /// <param name="siloIds">siloids given from the master region</param>
        /// <returns></returns>
        private ICollection<string> UpdateDeploymentRegionsToCurrentRegions(ICollection<string> siloIds)
        {
            var updatedSiloIds = new List<string>();

            foreach(string siloId in siloIds)
            {
                string updatedSiloId = $"{this.currentRegion}-{siloId.Substring(2)}";
                updatedSiloIds.Add(updatedSiloId);
            }

            return updatedSiloIds;
        }
    }
}