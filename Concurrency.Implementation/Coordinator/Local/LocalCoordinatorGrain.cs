using System;
using System.Collections.Generic;
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

namespace Concurrency.Implementation.Coordinator.Local
{
    /// <summary>
    /// Class for the API of the LocalCoordinator. The LocalCoordinator needs to be able to talk with both the TransactionExecutionGrain and
    /// the RegionalCoordinator
    /// </summary>
    [Reentrant]
    [LocalCoordinatorGrainPlacementStrategy]
    public class LocalCoordinatorGrain : Grain, ILocalCoordinatorGrain
    {
        private readonly ILogger<LocalCoordinatorGrain> logger;
        private readonly ILocalDeterministicTransactionProcessorFactory localDeterministicTransactionProcessorFactory;
        private ILocalDeterministicTransactionProcessor localDeterministicTransactionProcessor;
        private ILocalCoordinatorGrain neighbor;

        public LocalCoordinatorGrain(ILogger<LocalCoordinatorGrain> logger,
                                     ILocalDeterministicTransactionProcessorFactory localDeterministicTransactionProcessorFactory)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localDeterministicTransactionProcessorFactory = localDeterministicTransactionProcessorFactory ?? throw new ArgumentNullException(nameof(localDeterministicTransactionProcessorFactory));
        }

        public override Task OnActivateAsync()
        {
            this.localDeterministicTransactionProcessor = this.localDeterministicTransactionProcessorFactory.Create(this.GrainFactory, this.GrainReference);

            return Task.CompletedTask;
        }

        #region TransactionExecutionGrain
        public async Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos)
        {
            return await this.localDeterministicTransactionProcessor.NewLocalTransaction(grainAccessInfos);
        }

        public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfos)
        {
            return await this.localDeterministicTransactionProcessor.NewRegionalTransaction(regionalBid, regionalTid, grainAccessInfos);
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
            await this.localDeterministicTransactionProcessor.BatchCompletionAcknowledgement(bid);
        }

        #endregion

        #region LocalCoordinator
        public async Task WaitForBatchToCommit(long bid)
        {
            await this.localDeterministicTransactionProcessor.WaitForBatchToCommit(bid);
        }

        public async Task PassToken(LocalToken token)
        {
            long currentBatchId = this.localDeterministicTransactionProcessor.GenerateLocalBatch(token);
            IList<long> currentBatchIds = this.localDeterministicTransactionProcessor.GenerateRegionalBatch(token);

            Thread.Sleep(10);

            _ = this.neighbor.PassToken(token);
            if (currentBatchId != -1) await this.localDeterministicTransactionProcessor.EmitBatch(currentBatchId);

            if (currentBatchIds.Count > 0)
            {
                this.logger.LogInformation("BatchIds: {ids}", this.GrainReference, string.Join(", ", currentBatchIds));
            }

            foreach (var bid in currentBatchIds)
            {
                await this.localDeterministicTransactionProcessor.EmitBatch(bid);
            }
        }

        #endregion

        #region RegionalCoordinator

        public async Task RegionalBatchCommitAcknowledgement(long regionalBid)
        {
            await this.localDeterministicTransactionProcessor.AckRegionalBatchCommit(regionalBid);
        }

        public async Task ReceiveBatchSchedule(SubBatch batch)
        {
            await this.localDeterministicTransactionProcessor.ReceiveBatchSchedule(batch);
        }

        #endregion

        public Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor)
        {
            this.neighbor = neighbor;

            return Task.CompletedTask;
        }
    }
}