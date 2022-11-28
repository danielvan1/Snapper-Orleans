using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Concurrency.Implementation.Coordinator.Replica
{
    [Reentrant]
    [LocalCoordinatorGrainPlacementStrategy]
    public class LocalReplicaCoordinator : Grain, ILocalReplicaCoordinator
    {
        private Dictionary<long, int> expectedAcknowledgementsPerBatch;

        private Dictionary<long, Dictionary<GrainAccessInfo, LocalSubBatch>> bidToSchedules;
        private Dictionary<long, TaskCompletionSource<bool>> batchCommitPromises;
        private Dictionary<long, TaskCompletionSource<bool>> regionalbatchCommitPromises;
        private Dictionary<long, long> bidToPreviousBid;

        private IRegionalReplicaCoordinator regionalReplicaCoordinator;
        private string currentRegion;
        private readonly ILogger<LocalReplicaCoordinator> logger;

        private long highestCommittedRegionalBid;
        private long highestCommittedBid;

        public LocalReplicaCoordinator(ILogger<LocalReplicaCoordinator> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public override Task OnActivateAsync()
        {
            this.GetPrimaryKeyLong(out string siloId);
            this.currentRegion = siloId.Substring(0, 2);
            this.highestCommittedBid = -1;
            this.highestCommittedRegionalBid = -1;

            this.regionalReplicaCoordinator = this.GrainFactory.GetGrain<IRegionalReplicaCoordinator>(0, currentRegion);

            this.expectedAcknowledgementsPerBatch = new Dictionary<long, int>();
            this.bidToSchedules = new Dictionary<long, Dictionary<GrainAccessInfo, LocalSubBatch>>();
            this.batchCommitPromises = new Dictionary<long, TaskCompletionSource<bool>>();
            this.bidToPreviousBid = new Dictionary<long, long>();
            this.regionalbatchCommitPromises = new Dictionary<long, TaskCompletionSource<bool>>();
            // this.bidToPreviousBid.Add(0, -1);

            return Task.CompletedTask;
        }

        /// <summary>
        /// This is called from the home region of the transaction.
        /// </summary>
        /// <param name="bid"></param>
        /// <param name="previousBid"></param>
        /// <param name="schedule"></param>
        /// <returns></returns>
        public Task ReceiveLocalSchedule(long bid, long previousBid, Dictionary<GrainAccessInfo, LocalSubBatch> schedule)
        {
            this.logger.LogInformation("Received local schedules for batch {bid}, previous batch: {previous} and schedules: {schedule} from master",
                                        this.GrainReference, bid, previousBid, string.Join(", ", schedule.Select(kv => kv.Key + " :: " + kv.Value)));

            if(!this.expectedAcknowledgementsPerBatch.ContainsKey(bid))
            {
                this.expectedAcknowledgementsPerBatch[bid] = schedule.Count;
            }

            if(!this.bidToPreviousBid.ContainsKey(bid))
            {
                this.bidToPreviousBid[bid] = previousBid;
            }

            if(!this.bidToSchedules.ContainsKey(bid))
            {
                this.bidToSchedules.Add(bid, schedule);
            }

            foreach ((GrainAccessInfo grainId, LocalSubBatch localSubBatch) in schedule)
            {
                int id = grainId.Id;
                string masterRegion = grainId.SiloId;
                string replicaRegion = grainId.ReplaceDeploymentRegion(this.currentRegion);

                this.logger.LogInformation("Sending local schedule to Replica TransactionExecitionGrain: {grainId}-{replicaRegion}", this.GrainReference, id, replicaRegion);

                var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(id, replicaRegion, grainId.GrainClassNamespace);

                _ = destination.ReceiveBatchSchedule(localSubBatch);
            }

            return Task.CompletedTask;
        }


        /// <summary>
        /// Sent from the TransactionExecutionGrain that the current batch is done locally. Then check if all
        /// </summary>
        /// <param name="bid"></param>
        /// <returns></returns>
        public async Task CommitAcknowledgement(long bid)
        {
            this.logger.LogInformation("Received commit acknowledgment from replica grain for localbid: {localBid}. Current number of expected acknowledgements are: {acks}", this.GrainReference, bid, this.expectedAcknowledgementsPerBatch[bid]);
            this.expectedAcknowledgementsPerBatch[bid]--;

            if(this.expectedAcknowledgementsPerBatch[bid] > 0)
            {
                return;
            }
            this.logger.LogInformation("Herpderp1", this.GrainReference);

            LocalSubBatch localSubBatch = this.bidToSchedules[bid].First().Value;

            this.logger.LogInformation("Herpderp2", this.GrainReference);

            if(this.IsBatchRegional(localSubBatch))
            {
                this.logger.LogInformation("Current batch {bid} is regional in replica", this.GrainReference, bid);
                long regionalBid = localSubBatch.RegionalBid;
                await this.regionalReplicaCoordinator.CommitAcknowledgement(regionalBid);
                await this.WaitForRegionalBatchToCommit(regionalBid);
            }

            this.logger.LogInformation("Herpderp3", this.GrainReference);

            // When this is done we can start to commit the current batch
            await this.WaitForPreviousBatchToCommit(bid);
            // this.logger.LogInformation("Herpderp4", this.GrainReference);
            this.logger.LogInformation("Herpderp4", this.GrainReference);

            this.BatchCommitAcknowledgement(bid);
            this.logger.LogInformation("Herpderp5", this.GrainReference);

            // TODO: Change coordinator ID to this
            Dictionary<GrainAccessInfo, LocalSubBatch> currentScheduleMap = this.bidToSchedules[bid];
            this.logger.LogInformation("Herpderp6: currentschedulemapcount: {count}", this.GrainReference, currentScheduleMap.Count);

            // Sent message that the transaction grains can commit
            foreach ((GrainAccessInfo grainId, _) in currentScheduleMap)
            {
                this.GetPrimaryKeyLong(out string siloId);
                this.logger.LogInformation("Sending acknowledgement that local batch can commit to replica grain {grainId}-{siloId} for localbid: {localbid}", this.GrainReference, grainId.Id, siloId, bid);
                // this.logger.LogInformation($"Commit Grains", this.GrainReference);
                // Debug.Assert(region == grainId.Region); // I think this should be true, we just have the same info multiple places now
                var destination = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Id, siloId, grainId.GrainClassNamespace);
                _ = destination.AckBatchCommit(bid);
            }
        }

        public Task RegionalBatchCommitAcknowledgement(long regionalBid)
        {
            this.logger.LogInformation("RegionalBatchCommitAcknowledgement commit was called from regional coordinator. We can now commit regionalBatch: {regionalBid}",
                                       this.GrainReference, regionalBid);

            this.highestCommittedRegionalBid = Math.Max(regionalBid, this.highestCommittedRegionalBid);

            if (this.regionalbatchCommitPromises.ContainsKey(regionalBid))
            {
                this.regionalbatchCommitPromises[regionalBid].SetResult(true);
                this.regionalbatchCommitPromises.Remove(regionalBid);
            }

            return Task.CompletedTask;
        }


        private async Task WaitForPreviousBatchToCommit(long bid)
        {
            long previousBid = this.bidToPreviousBid[bid];
            this.logger.LogInformation("Current bid {bid} waiting for previous bid {prevBid} to commit", this.GrainReference, bid, previousBid);

            // This is when it is the first schedule.
            if(previousBid == -1) return;

            await this.WaitForBatchToCommit(previousBid);
        }

        private async Task WaitForRegionalBatchToCommit(long regionalBid)
        {
            if (this.highestCommittedRegionalBid >= regionalBid)
            {
                return;
            }

            if (!this.regionalbatchCommitPromises.ContainsKey(regionalBid))
            {
                this.regionalbatchCommitPromises.Add(regionalBid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("Waiting for the regional batch: {bid} to commit",
                                        this.GrainReference, regionalBid);

            // Waiting here for the RegionalCoordinator to sent a signal to commit for regionalBid.
            await this.regionalbatchCommitPromises[regionalBid].Task;

            this.logger.LogInformation("Done waiting for the regional batch: {bid} to commit",
                                        this.GrainReference, regionalBid);
        }

        private async Task WaitForBatchToCommit(long bid)
        {
            if (this.highestCommittedBid == bid) return;

            if (!this.batchCommitPromises.ContainsKey(bid))
            {
                this.batchCommitPromises.Add(bid, new TaskCompletionSource<bool>());
            }

            this.logger.LogInformation("Waiting for batch: {bid} to commit", this.GrainReference, bid);

            await this.batchCommitPromises[bid].Task;

            this.logger.LogInformation("Finish waiting for batch: {bid} to commit", this.GrainReference, bid);
        }

        private void BatchCommitAcknowledgement(long bid)
        {
            this.highestCommittedBid = Math.Max(bid, this.highestCommittedBid);

            this.logger.LogInformation("Batch {bid} can now commit", this.GrainReference, bid);

            if (this.batchCommitPromises.ContainsKey(bid))
            {
                // this.logger.LogInformation("Batch: {bid} can now commit", this.grainReference, bid);
                this.batchCommitPromises[bid].SetResult(true);
                this.batchCommitPromises.Remove(bid);
            }
        }

        private Task NotifyGrainsToCommit(long bid)
        {
            return Task.CompletedTask;
        }

        private bool IsBatchRegional(LocalSubBatch localSubBatch)
        {
            return localSubBatch.RegionalBid != -1;
        }
    }
}