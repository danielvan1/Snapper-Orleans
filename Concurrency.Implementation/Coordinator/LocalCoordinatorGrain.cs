using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [LocalCoordinatorGrainPlacementStrategy]
    public class LocalCoordinatorGrain : Grain, ILocalCoordinatorGrain
    {
        private string region;

        // coord basic info
        private int myID;
        private ILocalCoordinatorGrain neighborCoord;
        private Dictionary<Tuple<int, string>, string> grainClassName;                                             // grainID, grainClassName
        private readonly ILogger logger;

        // PACT
        private DetTxnProcessor detTxnProcessor;
        private Dictionary<long, int> expectedAcksPerBatch;
        private Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches;

        // Hierarchical Architecture
        // for global batches sent from global coordinators
        private SortedDictionary<long, SubBatch> globalBatchInfo;                                   // key: global bid
        private Dictionary<long, Dictionary<long, List<Tuple<int, string>>>> globalTransactionInfo;                // <global bid, <global tid, grainAccessInfo>>
        private Dictionary<long, TaskCompletionSource<Tuple<long, long>>> globalDetRequestPromise;  // <global tid, <local bid, local tid>>
        private Dictionary<long, long> localBidToGlobalBid;
        private Dictionary<long, Dictionary<long, long>> globalTidToLocalTidPerBatch;               // local bid, <global tid, local tid>

        // for global batch commitment
        private long highestCommittedGlobalBid;
        private Dictionary<long, int> globalBidToGlobalCoordID;
        private Dictionary<long, bool> globalBidToIsPrevBatchGlobal;                                // global bid, if this batch's previous one is also a global batch
        private Dictionary<long, TaskCompletionSource<bool>> globalBatchCommit;                     // global bid, commit promise

        // ACT
        private NonDetTxnProcessor nonDetTxnProcessor;
        private readonly SiloInfo SiloInfo;

        public LocalCoordinatorGrain(ILogger<LocalCoordinatorGrain> logger)
        {
            this.logger = logger;
        }

        void Init()
        {
            this.highestCommittedGlobalBid = -1;
            this.grainClassName = new Dictionary<Tuple<int, string>, string>();
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>>();
            this.globalBatchInfo = new SortedDictionary<long, SubBatch>();
            this.globalTransactionInfo = new Dictionary<long, Dictionary<long, List<Tuple<int, string>>>>();
            this.globalDetRequestPromise = new Dictionary<long, TaskCompletionSource<Tuple<long, long>>>();
            this.localBidToGlobalBid = new Dictionary<long, long>();
            this.globalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            this.globalBidToIsPrevBatchGlobal = new Dictionary<long, bool>();
            this.globalBatchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            this.globalBidToGlobalCoordID = new Dictionary<long, int>();
        }

        public override Task OnActivateAsync()
        {
            this.Init();
            this.myID = (int)this.GetPrimaryKeyLong(out _);
            this.nonDetTxnProcessor = new NonDetTxnProcessor(myID);
            this.detTxnProcessor = new DetTxnProcessor(
                this.logger,
                this.GrainReference,
                this.myID,
                this.expectedAcksPerBatch,
                this.bidToSubBatches);
            this.GetPrimaryKeyLong(out string region);
            this.region = region;
            this.logger.LogInformation($"Local coordinator was activated in {this.region}", this.GrainReference);
            return base.OnActivateAsync();
        }


        // Going to be called by our Regional Coordinator
        public Task ReceiveBatchSchedule(SubBatch batch)
        {
            var globalBid = batch.bid;
            this.globalBatchInfo.Add(globalBid, batch);
            this.globalBidToGlobalCoordID.Add(globalBid, batch.coordID);

            if (!this.globalTransactionInfo.ContainsKey(globalBid))
            {
                this.globalTransactionInfo.Add(globalBid, new Dictionary<long, List<Tuple<int, string>>>());
            }
            return Task.CompletedTask;
        }

        public async Task<TransactionRegistInfo> NewGlobalTransaction(long globalBid, long globalTid, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName)
        {
            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grainID = grainAccessInfo[i];
                if (this.grainClassName.ContainsKey(grainID) == false)
                    this.grainClassName.Add(grainID, grainClassName[i]);
            }

            if (this.globalTransactionInfo.ContainsKey(globalBid) == false)
                this.globalTransactionInfo.Add(globalBid, new Dictionary<long, List<Tuple<int, string>>>());
            this.globalTransactionInfo[globalBid].Add(globalTid, grainAccessInfo);

            var promise = new TaskCompletionSource<Tuple<long, long>>();
            this.globalDetRequestPromise.Add(globalTid, promise);
            await promise.Task;
            return new TransactionRegistInfo(promise.Task.Result.Item1, promise.Task.Result.Item2, this.detTxnProcessor.highestCommittedBid);
        }

        // for PACT
        public async Task<TransactionRegistInfo> NewTransaction(List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName)
        {
            this.GetPrimaryKeyLong(out string region);
            this.logger.LogInformation($"NewTransaction is called on local coordinator: {region}", this.GrainReference);
            var task = this.detTxnProcessor.NewDet(grainAccessInfo);
            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grain = grainAccessInfo[i];
                if (this.grainClassName.ContainsKey(grain) == false)
                    this.grainClassName.Add(grain, grainClassName[i]);
            }
            var id = await task;
            this.logger.LogInformation($"NewTransaction is going to return tid: {id.Item1} and {id.Item2}", this.GrainReference);
            return new TransactionRegistInfo(id.Item1, id.Item2, this.detTxnProcessor.highestCommittedBid);
        }

        // for ACT
        public async Task<TransactionRegistInfo> NewTransaction()
        {
            var tid = await this.nonDetTxnProcessor.NewNonDet();
            return new TransactionRegistInfo(tid, this.detTxnProcessor.highestCommittedBid);
        }

        public async Task PassToken(LocalToken token)
        {
            /*if (this.region.Equals("EU-EU-1"))
            {
                this.logger.Info($"PassToken is called on region:{this.region}");
            }*/
            long curBatchID;
            var curBatchIDs = new List<long>();
            if (token.isLastEmitBidGlobal)
            {
                this.ProcessGlobalBatch(token, curBatchIDs);
                curBatchID = this.detTxnProcessor.GenerateBatch(token);
            }
            else
            {
                /*if (this.region.Equals("EU-EU-1"))
                {
                    this.logger.Info($"LocalCoordinator in region {this.region} is going to call GenerateBatch");
                }*/
                curBatchID = this.detTxnProcessor.GenerateBatch(token);
                this.ProcessGlobalBatch(token, curBatchIDs);
            }

            this.nonDetTxnProcessor.EmitNonDetTransactions(token);

            if (this.detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
                this.detTxnProcessor.GarbageCollectTokenInfo(token);
            else this.detTxnProcessor.highestCommittedBid = token.highestCommittedBid;

            _ = this.neighborCoord.PassToken(token);
            if (curBatchID != -1) await EmitBatch(curBatchID);
            if (curBatchIDs.Count != 0)
                foreach (var bid in curBatchIDs) await EmitBatch(bid);
        }

        void ProcessGlobalBatch(LocalToken token, List<long> curBatchIDs)
        {
            while (this.globalBatchInfo.Count != 0)
            {
                var batch = this.globalBatchInfo.First();
                var globalBid = batch.Key;

                if (batch.Value.lastBid != token.lastEmitGlobalBid) return;
                if (batch.Value.txnList.Count != this.globalTransactionInfo[globalBid].Count) return;

                var curBatchID = token.lastEmitTid + 1;
                curBatchIDs.Add(curBatchID);
                this.localBidToGlobalBid.Add(curBatchID, globalBid);
                this.globalTidToLocalTidPerBatch.Add(curBatchID, new Dictionary<long, long>());

                foreach (var globalTid in batch.Value.txnList)
                {
                    var localTid = ++token.lastEmitTid;
                    this.globalDetRequestPromise[globalTid].SetResult(new Tuple<long, long>(curBatchID, localTid));

                    var grainAccessInfo = globalTransactionInfo[globalBid][globalTid];
                    this.detTxnProcessor.GenerateSchedulePerService(localTid, curBatchID, grainAccessInfo);

                    this.globalTidToLocalTidPerBatch[curBatchID].Add(globalTid, localTid);
                    this.globalDetRequestPromise.Remove(globalTid);
                }
                this.globalBidToIsPrevBatchGlobal.Add(globalBid, token.isLastEmitBidGlobal);
                this.globalBatchInfo.Remove(globalBid);
                this.globalTransactionInfo.Remove(globalBid);
                this.detTxnProcessor.UpdateToken(token, curBatchID, globalBid);
                token.lastEmitGlobalBid = globalBid;
            }
        }

        async Task EmitBatch(long bid)
        {
            var curScheduleMap = this.bidToSubBatches[bid];

            long globalBid = -1;
            if (this.localBidToGlobalBid.ContainsKey(bid))
            {
                globalBid = this.localBidToGlobalBid[bid];
            }

            var globalTidToLocalTid = new Dictionary<long, long>();
            if (this.globalTidToLocalTidPerBatch.ContainsKey(bid))
            {
                globalTidToLocalTid = this.globalTidToLocalTidPerBatch[bid];
                this.globalTidToLocalTidPerBatch.Remove(bid);
            }

            foreach (var item in curScheduleMap)
            {
                this.GetPrimaryKeyLong(out string region);
                this.logger.LogInformation($"LocalCoordinatorGrain calling EmitBatch on transaction execution grain: {item.Key} region: {region} ", this.GrainReference);
                // I think this should be true, we just have the same info multiple places now
                // The problem is if this is not true, then the local coordinator is talking to
                // grains in other servers
                Debug.Assert(region == item.Key.Item2); // I think this should be true, we just have the same info multiple places now
                var dest = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.Item1, region, this.grainClassName[item.Key]);
                var batch = item.Value;

                var localSubBatch = new LocalSubBatch(globalBid, batch);
                localSubBatch.highestCommittedBid = this.detTxnProcessor.highestCommittedBid;
                localSubBatch.globalTidToLocalTid = globalTidToLocalTid;

                _ = dest.ReceiveBatchSchedule(localSubBatch);
            }
        }

        // TODO: Rename to AckRegionalCoordinator, and input RegionalBatchId ?
        public void ACKRegionalCoordinator(long globalBid)
        {
            this.GetPrimaryKeyLong(out string region);
            var globalCoordID = globalBidToGlobalCoordID[globalBid];
            // Just try to get the regional silo somehow to see if it works
            var regionalCoordinatorRegion = region.Substring(0, 2);
            //this.logger.Info($"[{region}] LocalCoordinatorGrain is going to call AckBatchCompletion on the regional coordinator:{regionalCoordinatorRegion} ID:{globalCoordID}");
             var regionalCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(globalCoordID, regionalCoordinatorRegion);
            _ = regionalCoordinator.AckBatchCompletion(globalBid);
        }

        public async Task AckBatchCompletion(long bid)
        {
            this.logger.LogInformation($"Expected acknowledgements for batch:{bid} before decrement: {this.expectedAcksPerBatch[bid]}", this.GrainReference);
            this.expectedAcksPerBatch[bid]--;
            this.logger.LogInformation($"Expected acknowledgements for batch:{bid} after decrement: {this.expectedAcksPerBatch[bid]}", this.GrainReference);

            if (expectedAcksPerBatch[bid] != 0) return;

            // the batch has been completed in this silo
            long globalBid = -1;
            var isPrevGlobal = false;
            var isGlobal = localBidToGlobalBid.ContainsKey(bid);

            if (isGlobal)
            {
                // ACK the regional coordinator
                globalBid = localBidToGlobalBid[bid];
                isPrevGlobal = globalBidToIsPrevBatchGlobal[globalBid];
                if (isPrevGlobal)
                {
                    this.ACKRegionalCoordinator(globalBid);
                }
            }

            await this.detTxnProcessor.WaitPrevBatchToCommit(bid);

            if (isGlobal)
            {
                if (!isPrevGlobal) 
                {
                    this.ACKRegionalCoordinator(globalBid);
                } 
                await WaitGlobalBatchCommit(globalBid);

                this.localBidToGlobalBid.Remove(bid);
                this.globalBidToGlobalCoordID.Remove(globalBid);
                this.globalBidToIsPrevBatchGlobal.Remove(globalBid);
            }

            this.detTxnProcessor.AckBatchCommit(bid);

            var currentScheduleMap = bidToSubBatches[bid];
            foreach (var item in currentScheduleMap)
            {
                this.GetPrimaryKeyLong(out string region);
                this.logger.LogInformation($"LocalCoordinator calls commit on actors", this.GrainReference);
                Debug.Assert(region == item.Key.Item2); // I think this should be true, we just have the same info multiple places now
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.Item1, region, this.grainClassName[item.Key]);
                _ = dest.AckBatchCommit(bid);
            }

            this.bidToSubBatches.Remove(bid);
            this.expectedAcksPerBatch.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            await this.detTxnProcessor.WaitBatchCommit(bid);
        }

        async Task WaitGlobalBatchCommit(long globalBid)
        {
            if (this.highestCommittedGlobalBid >= globalBid) 
            {
                return;
            }
            if (!this.globalBatchCommit.ContainsKey(globalBid)) 
            {
                this.globalBatchCommit.Add(globalBid, new TaskCompletionSource<bool>());
            }
                
            await this.globalBatchCommit[globalBid].Task;
        }

        public Task AckGlobalBatchCommit(long globalBid)
        {
            this.highestCommittedGlobalBid = Math.Max(globalBid, highestCommittedGlobalBid);
            if (this.globalBatchCommit.ContainsKey(globalBid))
            {
                this.globalBatchCommit[globalBid].SetResult(true);
                this.globalBatchCommit.Remove(globalBid);
            }
            return Task.CompletedTask;
        }

        public Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor)
        {
            this.highestCommittedGlobalBid = -1;
            this.detTxnProcessor.Init();
            this.nonDetTxnProcessor.Init();

            this.neighborCoord = neighbor;

            return Task.CompletedTask;
        }
    }
}
