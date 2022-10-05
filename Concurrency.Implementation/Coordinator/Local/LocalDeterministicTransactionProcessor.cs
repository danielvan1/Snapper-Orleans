using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator.Models;
using Concurrency.Implementation.LoadBalancing;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Utilities;

namespace Concurrency.Implementation.Coordinator.Local
{
    public class LocalDeterministicTransactionProcessor : ILocalDeterministicTransactionProcessor
    {
        private readonly ILogger<LocalDeterministicTransactionProcessor> logger;
        private readonly ICoordinatorProvider coordinatorProvider;
        private readonly IGrainFactory grainFactory;
        private readonly GrainReference grainReference;

        private Dictionary<long, int> expectedAcksPerBatch;

        private readonly Dictionary<long, LocalBatchProcessInfo> localBatchProcessInfos;
        private readonly Dictionary<long, RegionalBatchProcessInfo> regionalBatchProcessInfos;

        private readonly List<List<GrainAccessInfo>> deterministicTransactionRequests;
        private readonly List<TaskCompletionSource<TransactionId>> deterministicTransactionRequestPromises; // <local bid, local tid>
        private readonly SortedDictionary<long, SubBatch> regionalBatchInfos;                // key: regional bid
        private readonly Dictionary<long, TaskCompletionSource<TransactionId>> regionalDeterministicTransactionRequestPromises;  // <regional tid, <local bid, local tid>>

        private long highestCommittedRegionalBid;
        private long highestCommittedBid;

        public LocalDeterministicTransactionProcessor(ILogger<LocalDeterministicTransactionProcessor> logger,
                                                      ICoordinatorProvider regionalCoordinatorProvider,
                                                      IGrainFactory grainFactory,
                                                      GrainReference grainReference)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.coordinatorProvider = regionalCoordinatorProvider ?? throw new ArgumentNullException(nameof(regionalCoordinatorProvider));
            this.grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
            this.grainReference = grainReference ?? throw new ArgumentNullException(nameof(grainReference));

            this.highestCommittedBid = -1;
            this.highestCommittedRegionalBid = -1;

            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.localBatchProcessInfos = new Dictionary<long, LocalBatchProcessInfo>();

            this.deterministicTransactionRequests = new List<List<GrainAccessInfo>>();
            this.deterministicTransactionRequestPromises = new List<TaskCompletionSource<TransactionId>>();

            this.regionalBatchInfos = new SortedDictionary<long, SubBatch>();
            this.regionalDeterministicTransactionRequestPromises = new Dictionary<long, TaskCompletionSource<TransactionId>>();
            this.regionalBatchProcessInfos = new Dictionary<long, RegionalBatchProcessInfo>();
        }

        public async Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos)
        {
            this.logger.LogInformation("NewLocalTransaction is called with grainAccessInfo: [{grainAccessInfo}]",
                                       this.grainReference, string.Join(", ", grainAccessInfos));

            this.deterministicTransactionRequests.Add(grainAccessInfos);
            var transactionIdPromise = new TaskCompletionSource<TransactionId>();

            // We are waiting until the token is arrived which then will create the tuple.
            this.deterministicTransactionRequestPromises.Add(transactionIdPromise);

            this.logger.LogInformation("Waiting for the token to arrive. Size: {size}", this.grainReference, this.deterministicTransactionRequests.Count);
            var transactionId = await transactionIdPromise.Task;

            long bid = transactionId.Bid;
            long tid = transactionId.Tid;

            this.logger.LogInformation("NewLocalTransaction is going to return bid: {bid} and tid: {tid}", this.grainReference, bid, tid);

            return new TransactionRegisterInfo(bid, tid, this.highestCommittedBid);
        }

        // TODO: We should inject a CoordinatorProvider such that we can choose a regionalCoordinator
        public async Task<TransactionRegisterInfo> NewRegionalTransaction(long regionalBid, long regionalTid, List<GrainAccessInfo> grainAccessInfo)
        {
            // this.grainReference.GetPrimaryKeyLong(out string region1);
            // string region = region1.Substring(0, 2);

            // IRegionalCoordinatorGrain regionalCoordinator = this.regionalCoordinatorProvider.GetCoordinator(region);
            this.logger.LogInformation("NewRegionalTransaction is called regionalBid {globalBid} and regionalTid {tid}", this.grainReference, regionalBid, regionalTid);

            if (!this.regionalBatchProcessInfos.TryGetValue(regionalBid, out RegionalBatchProcessInfo regionalBatchProcessInfo))
            {
                this.regionalBatchProcessInfos.Add(regionalBid, regionalBatchProcessInfo = new RegionalBatchProcessInfo());
            }

            regionalBatchProcessInfo.RegionalTransactionInfos.Add(regionalTid, grainAccessInfo);

            var transactionIdPromise = new TaskCompletionSource<TransactionId>();
            this.logger.LogInformation("Waiting for regional transactionIdPromise {promise} to bet set to a value", this.grainReference, transactionIdPromise);
            this.regionalDeterministicTransactionRequestPromises.Add(regionalTid, transactionIdPromise);

            TransactionId transactionId = await transactionIdPromise.Task;

            this.logger.LogInformation("Done waiting for promise {promise} to bet set to a value", this.grainReference, transactionIdPromise);

            return new TransactionRegisterInfo(transactionId.Bid, transactionId.Tid, highestCommittedBid);
        }

        /// <summary>
        /// This is called every time the corresponding coordinator receives the token.
        /// </summary>
        /// <returns>batchId</returns>
        public long GenerateLocalBatch(LocalToken token)
        {
            if (this.deterministicTransactionRequests.Count == 0)
            {
                return -1;
            }

            this.logger.LogInformation("GenerateLocalBatch2: {size}", this.grainReference, this.deterministicTransactionRequests.Count);

            // assign bid and tid to waited PACTs
            var currentBatchId = token.PreviousEmitTid + 1;
            this.localBatchProcessInfos.TryAdd(currentBatchId, new LocalBatchProcessInfo());

            for (int i = 0; i < this.deterministicTransactionRequests.Count; i++)
            {
                var tid = ++token.PreviousEmitTid;
                // Be aware that currently it has to be side effects!
                this.GenerateSchedulePerGrain(this.localBatchProcessInfos[currentBatchId].SchedulePerGrain, tid, currentBatchId, this.deterministicTransactionRequests[i]);

                this.logger.LogInformation("herpderp count: {count}, currentBatchId: {currentBatchId}", this.localBatchProcessInfos[currentBatchId].SchedulePerGrain.Count, currentBatchId);

                var transactionId = new TransactionId() { Tid = tid, Bid = currentBatchId };

                this.deterministicTransactionRequestPromises[i].SetResult(transactionId);
                this.logger.LogInformation("Setting transactionId to a value: {transactionId}", this.grainReference, transactionId);
            }

            this.UpdateToken(token, currentBatchId, -1);

            this.deterministicTransactionRequests.Clear();
            this.deterministicTransactionRequestPromises.Clear();
            this.HerpDerp(token);

            return currentBatchId;
        }

        public IList<long> GenerateRegionalBatch(LocalToken token)
        {
            IList<long> currentBatchIds = new List<long>();

            while (this.regionalBatchInfos.Count > 0)
            {
                var bidAndBatch = this.regionalBatchInfos.First();
                var regionalBid = bidAndBatch.Key;
                var subBatch = bidAndBatch.Value;

                RegionalBatchProcessInfo regionalBatchProcessInfo = this.regionalBatchProcessInfos[regionalBid];

                if (subBatch.PreviousBid != token.LastEmitGlobalBid)
                {
                    return new List<long>();
                }

                if (subBatch.Transactions.Count != regionalBatchProcessInfo.RegionalTransactionInfos.Count)
                {
                    return new List<long>();
                }

                this.logger.LogInformation("HerpDerp regionalBid: {regionalBid}, subbatch: {batch}", this.grainReference, regionalBid, subBatch);

                var currentBatchId = token.PreviousEmitTid + 1;
                this.localBatchProcessInfos.TryAdd(currentBatchId, new LocalBatchProcessInfo());
                this.regionalBatchProcessInfos.TryAdd(regionalBid, new RegionalBatchProcessInfo());

                LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[currentBatchId];
                localBatchProcessInfo.RegionalBid = regionalBid;
                currentBatchIds.Add(currentBatchId);

                foreach (var regionalTid in subBatch.Transactions)
                {
                    var localTid = ++token.PreviousEmitTid;
                    TransactionId transactionId = new TransactionId() { Bid = currentBatchId, Tid = localTid };
                    this.regionalDeterministicTransactionRequestPromises[regionalTid].SetResult(transactionId);

                    var grainAccessInfo = this.regionalBatchProcessInfos[regionalBid].RegionalTransactionInfos[regionalTid];

                    this.GenerateSchedulePerGrain(localBatchProcessInfo.SchedulePerGrain, localTid, currentBatchId, grainAccessInfo);


                    this.logger.LogInformation("GenerateRegionalBatch: Count: {count} --- schedulePerGrain: [{grain}]", this.grainReference, localBatchProcessInfo.SchedulePerGrain.Count, string.Join(";; ", localBatchProcessInfo.SchedulePerGrain.Select(kv => kv.Key + ": " + kv.Value)));

                    localBatchProcessInfo.RegionalToLocalTidMapping.Add(regionalTid, localTid);


                    this.regionalDeterministicTransactionRequestPromises.Remove(regionalTid);
                }

                regionalBatchProcessInfo.IsPreviousBatchRegional = token.IsLastEmitBidRegional;

                this.regionalBatchInfos.Remove(regionalBid);

                this.UpdateToken(token, currentBatchId, regionalBid);

                token.LastEmitGlobalBid = regionalBid;
            }

            this.HerpDerp(token);

            return currentBatchIds;
        }

        private void GenerateSchedulePerGrain(IDictionary<GrainAccessInfo, SubBatch> schedulePerGrain, long tid, long currentBatchId,
                                                                               List<GrainAccessInfo> deterministicTransactionRequests)
        {
            this.logger.LogInformation("GenerateSchedulePerService: Number of transactionRequest: {count}. The grains accessed are: {grains}", this.grainReference, deterministicTransactionRequests.Count, string.Join(", ", deterministicTransactionRequests));

            // var schedulePerGrain = new Dictionary<GrainAccessInfo, SubBatch>();

            for (int i = 0; i < deterministicTransactionRequests.Count; i++)
            {
                var grainId = deterministicTransactionRequests[i];

                if (!schedulePerGrain.ContainsKey(grainId))
                {
                    schedulePerGrain.Add(grainId, new SubBatch(currentBatchId, this.grainReference.GetPrimaryKeyLong(out _)));
                }

                schedulePerGrain[grainId].Transactions.Add(tid);
            }

            this.logger.LogInformation("GenerateSchedulePerService Done", this.grainReference);

            // return schedulePerGrain;
        }

        public Task EmitBatch(long bid)
        {
            IDictionary<GrainAccessInfo, SubBatch> currentScheduleMap = this.localBatchProcessInfos[bid].SchedulePerGrain;
            this.logger.LogInformation("localbatchprocessinfos: {infos}, bid: {bid}", this.grainReference, string.Join(", ", currentScheduleMap.Select(kv => kv.Key + " : " + kv.Value)), bid);

            long regionalBid = this.localBatchProcessInfos.ContainsKey(bid) ? this.localBatchProcessInfos[bid].RegionalBid : -1;
            this.logger.LogInformation("Contains: {c}, regionalBid: {regionalBid}, value: {value}, count: {count}", this.grainReference, this.localBatchProcessInfos.ContainsKey(bid), regionalBid, this.localBatchProcessInfos[bid].RegionalBid, currentScheduleMap.Count);

            var regionalTidToLocalTid = new Dictionary<long, long>();

            if (this.localBatchProcessInfos.ContainsKey(bid))
            {
                regionalTidToLocalTid = this.localBatchProcessInfos[bid].RegionalToLocalTidMapping;
            }

            foreach ((var grainAccessInfo , SubBatch subBatch) in currentScheduleMap)
            {
                int id = grainAccessInfo.Id;
                string region = grainAccessInfo.Region;

                this.logger.LogInformation("Calling EmitBatch on transaction execution grain {grain} with regionalbid: {regionalBid} ", this.grainReference, grainAccessInfo, regionalBid);

                // I think this should be true, we just have the same info multiple places now
                // The problem is if this is not true, then the local coordinator is talking to
                // grains in other servers

                var destination = this.grainFactory.GetGrain<ITransactionExecutionGrain>(id, region, grainAccessInfo.GrainClassName);

                var localSubBatch = new LocalSubBatch(subBatch)
                {
                    RegionalBid = regionalBid,
                    HighestCommittedBid = highestCommittedBid,
                    RegionalTidToLocalTid = regionalTidToLocalTid
                };

                _ = destination.ReceiveBatchSchedule(localSubBatch);
            }

            return Task.CompletedTask;
        }

        public void UpdateToken(LocalToken token, long currentBatchId, long globalBid)
        {
            // Here we assume that every actor is only called once
            LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[currentBatchId];
            IDictionary<GrainAccessInfo, SubBatch> grainIdToSubBatch = localBatchProcessInfo.SchedulePerGrain;
            this.logger.LogInformation("UpdateToken: Count: {count} --- grainIdToSubBatch {dict}", this.grainReference, string.Join(", ", grainIdToSubBatch.Select(kv => kv.Key + ": " + kv.Value)), grainIdToSubBatch.Count);
            this.expectedAcksPerBatch.Add(currentBatchId, grainIdToSubBatch.Count);
            this.logger.LogInformation("UpdateToken: for current batch: {bid} and token: {token}", this.grainReference, currentBatchId, token);

            // update the previous batch ID for each service accessed by this batch
            foreach ((GrainAccessInfo grainAccessInfo, SubBatch subBatch) in grainIdToSubBatch)
            {
                this.logger.LogInformation("Grain: {grainId} and subbatch: {subbatch}", this.grainReference, grainAccessInfo, subBatch);

                if (token.PreviousBidPerGrain.ContainsKey(grainAccessInfo))
                {
                    this.logger.LogInformation("New subbatch previousBid value: {value}", this.grainReference, token.PreviousBidPerGrain[grainAccessInfo]);
                    subBatch.PreviousBid = token.PreviousBidPerGrain[grainAccessInfo];
                    subBatch.previousGlobalBid = token.PreviousRegionalBidPerGrain[grainAccessInfo];
                }
                // else, the default value is -1

                Debug.Assert(subBatch.Bid > subBatch.PreviousBid);
                token.PreviousBidPerGrain[grainAccessInfo] = subBatch.Bid;
                token.PreviousRegionalBidPerGrain[grainAccessInfo] = globalBid;
            }


            localBatchProcessInfo.PreviousBid = token.PreviousEmitBid;

            if (token.PreviousEmitBid != -1)
            {
                localBatchProcessInfo.PreviousCoordinatorId = token.PreviousCoordinatorId;
            }

            token.PreviousEmitBid = currentBatchId;
            token.IsLastEmitBidRegional = globalBid != -1;
            token.PreviousCoordinatorId = this.grainReference.GetPrimaryKeyLong(out string _);

            this.logger.LogInformation("updated token: {token}", this.grainReference, token);
        }

        public async Task WaitForPreviousBatchToCommit(long bid)
        {
            var localBatchProcessInfo = this.localBatchProcessInfos[bid];
            var previousBid = localBatchProcessInfo.PreviousBid;
            this.logger.LogInformation("Waiting for previous batch: {prevBid} to commit. Current bid: {bid}", this.grainReference, previousBid, bid);

            if (this.highestCommittedBid < previousBid)
            {
                var coordinator = localBatchProcessInfo.PreviousCoordinatorId;
                if (coordinator == this.grainReference.GetPrimaryKeyLong(out _))
                {
                    await this.WaitForBatchToCommit(previousBid);
                }
                else
                {
                    this.logger.LogInformation("FUCKING HERP DERP", this.grainReference);
                    this.grainReference.GetPrimaryKeyLong(out string region);
                    var previousBatchCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordinator, region);
                    await previousBatchCoordinator.WaitForBatchToCommit(previousBid);
                }
            }
            else
            {
                Debug.Assert(this.highestCommittedBid == previousBid);
            }

            this.logger.LogInformation("Finished waiting for previous batch: {prevBid} to finish. Current bid: {bid}", this.grainReference, previousBid, bid);
        }

        public async Task WaitForBatchToCommit(long bid)
        {
            LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[bid];

            if (this.highestCommittedBid == bid) return;

            if (localBatchProcessInfo.BatchCommitPromise is null)
            {
                localBatchProcessInfo.BatchCommitPromise = new TaskCompletionSource<bool>();
            }

            this.logger.LogInformation("Waiting for batch: {bid} to commit", this.grainReference, bid);

            await localBatchProcessInfo.BatchCommitPromise.Task;

            this.logger.LogInformation("Finish waiting for batch: {bid} to commit", this.grainReference, bid);
        }

        public Task BatchCommitAcknowledgement(long bid)
        {
            LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[bid];

            this.highestCommittedBid = Math.Max(bid, highestCommittedBid);

            if (localBatchProcessInfo.BatchCommitPromise is not null)
            {

                this.logger.LogInformation("Batch: {bid} can now commit", this.grainReference, bid);
                localBatchProcessInfo.BatchCommitPromise.SetResult(true);
            }

            return Task.CompletedTask;
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
            this.logger.LogInformation("Expected acknowledgements for batch: {bid} before decrement: {expectedAcksPerBatch}", this.grainReference, bid, this.expectedAcksPerBatch[bid]);
            this.expectedAcksPerBatch[bid]--;

            if (this.expectedAcksPerBatch[bid] > 0) return;


            // the batch has been completed in this silo
            long regionalBid = -1;

            // TODO: Why does it matter why the previous is a regional or local batch?
            bool isPreviousRegional = false;
            LocalBatchProcessInfo localBatchProcessInfo = this.localBatchProcessInfos[bid];
            bool isRegional = localBatchProcessInfo.IsRegional;

            this.logger.LogInformation("Got all acknowledgements for batch: {bid}. Is the batch regional: {isRegional} and is the previous batch regional: {isPreviousRegional}",
                                        this.grainReference, bid, isRegional, isPreviousRegional);

            // Some sick optimization.......
            if (isRegional)
            {
                // ACK the regional coordinator
                regionalBid = localBatchProcessInfo.RegionalBid;
                RegionalBatchProcessInfo regionalBatchProcessInfo = this.regionalBatchProcessInfos[regionalBid];
                isPreviousRegional = regionalBatchProcessInfo.IsPreviousBatchRegional;

                if (isPreviousRegional)
                {
                    this.AckCompletionToRegionalCoordinator(regionalBid);

                }
            }

            await this.WaitForPreviousBatchToCommit(bid);

            if (isRegional)
            {
                if (!isPreviousRegional)
                {
                    this.AckCompletionToRegionalCoordinator(regionalBid);
                }

                await this.WaitForRegionalBatchToCommit(regionalBid);
            }

            // TODO: Check if this is correct
            await this.BatchCommitAcknowledgement(bid);

            IDictionary<GrainAccessInfo, SubBatch> currentScheduleMap = localBatchProcessInfo.SchedulePerGrain;

            // Sent message that the transaction grains can commit
            foreach ((GrainAccessInfo grainId, SubBatch subBatch) in currentScheduleMap)
            {
                this.grainReference.GetPrimaryKeyLong(out string region);
                this.logger.LogInformation($"Commit Grains", this.grainReference);
                Debug.Assert(region == grainId.Region); // I think this should be true, we just have the same info multiple places now

                var destination = this.grainFactory.GetGrain<ITransactionExecutionGrain>(grainId.Id, region, grainId.GrainClassName);
                _ = destination.AckBatchCommit(bid);
            }

            this.expectedAcksPerBatch.Remove(bid);
            // this.localBatchProcessInfos.Remove(bid);
            if(regionalBid != -1) this.regionalBatchProcessInfos.Remove(regionalBid);
        }

        private void AckCompletionToRegionalCoordinator(long regionalBid)
        {
            this.grainReference.GetPrimaryKeyLong(out string region);
            string region1 = new string(region);
            var regionalCoordID = this.regionalBatchProcessInfos[regionalBid].RegionalCoordinatorId;
            // Just try to get the regional silo somehow to see if it works
            string regionalCoordinatorRegion = region1.Substring(0, 2);
            this.logger.LogInformation("Complete current regional batch: {regionalBid} to RegionalCoordinator {id}-{region}", this.grainReference, regionalBid, regionalCoordID, regionalCoordinatorRegion);
            //this.logger.Info($"[{region}] LocalCoordinatorGrain is going to call AckBatchCompletion on the regional coordinator:{regionalCoordinatorRegion} ID:{globalCoordID}");
            var regionalCoordinator = this.grainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordID, regionalCoordinatorRegion);

            _ = regionalCoordinator.AckBatchCompletion(regionalBid);
        }

        private async Task WaitForRegionalBatchToCommit(long regionalBid)
        {
            if (this.highestCommittedRegionalBid >= regionalBid)
            {
                return;
            }

            this.logger.LogInformation("Waiting for the regional batch: {bid} to commit",
                                        this.grainReference, regionalBid);

            // Waiting here for the RegionalCoordinator to sent a signal to commit for regionalBid.
            await this.regionalBatchProcessInfos[regionalBid].BatchCommitPromise.Task;
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
                                       this.grainReference,  regionalBid);

            this.highestCommittedRegionalBid = Math.Max(regionalBid, this.highestCommittedRegionalBid);

            RegionalBatchProcessInfo regionalBatchProcessInfo = this.regionalBatchProcessInfos[regionalBid];

            // TODO: Check this is correct
            regionalBatchProcessInfo.BatchCommitPromise.SetResult(true);

            return Task.CompletedTask;
        }

        public Task ReceiveBatchSchedule(SubBatch batch)
        {
            this.logger.LogInformation("Received batch: {batch} schedule from regional coordinator",
                                       this.grainReference, batch);

            var regionalBid = batch.Bid;
            this.logger.LogInformation("RegionalBatchProcessInfo: {info}",
                                        this.grainReference, string.Join(", ", this.regionalBatchProcessInfos.Select(kv => kv.Key + ": " + kv.Value)));
            RegionalBatchProcessInfo regionalBatchProcessInfo = this.regionalBatchProcessInfos[regionalBid];

            regionalBatchProcessInfo.RegionalSubBatch = batch;
            regionalBatchProcessInfo.RegionalCoordinatorId = batch.CoordinatorId;

            this.regionalBatchInfos.Add(regionalBid, batch);

            this.logger.LogInformation("All values sat in ReceiveBatchSchedule", this.grainReference);

            return Task.CompletedTask;
        }

        public void GarbageCollectTokenInfo(LocalToken token)
        {
            var expiredGrains = new HashSet<GrainAccessInfo>();

            // only when last batch is already committed, the next emitted batch can have its lastBid = -1 again
            foreach (var item in token.PreviousBidPerGrain)
            {
                if (item.Value <= highestCommittedBid)
                {
                     expiredGrains.Add(item.Key);
                }
            }

            foreach (var item in expiredGrains)
            {
                token.PreviousBidPerGrain.Remove(item);
                token.PreviousRegionalBidPerGrain.Remove(item);
            }

            token.HighestCommittedBid = highestCommittedBid;
        }

        private void HerpDerp(LocalToken token)
        {
            if (this.highestCommittedBid > token.HighestCommittedBid)
            {
                GarbageCollectTokenInfo(token);
            }
            else
            {
                this.highestCommittedBid = token.HighestCommittedBid;
            }
        }

    }
}