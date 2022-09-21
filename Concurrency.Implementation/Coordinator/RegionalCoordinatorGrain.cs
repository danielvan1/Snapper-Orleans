using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        int myID;
        ICoordMap coordMap;
        IRegionalCoordinatorGrain neighborCoord;
        private readonly ILogger logger;

        // PACT
        DetTxnProcessor detTxnProcessor;
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch;        // <global bid, siloID, chosen local Coord ID>

        // ACT
        NonDetTxnProcessor nonDetTxnProcessor;

        DateTime timeOfBatchGeneration;
        double batchSizeInMSecs;

        public RegionalCoordinatorGrain(ILogger logger)
        {
            this.logger = logger;
        }

        public Task CheckGC()
        {
            detTxnProcessor.CheckGC();
            nonDetTxnProcessor.CheckGC();
            if (expectedAcksPerBatch.Count != 0) this.logger.LogInformation($"Regional Coordinator {myID}: expectedAcksPerBatch.Count = {expectedAcksPerBatch.Count}");
            if (bidToSubBatches.Count != 0) this.logger.LogInformation($"Regional Coordinator {myID}: batchSchedulePerSilo.Count = {bidToSubBatches.Count}");
            if (localCoordinatorPerSiloPerBatch.Count != 0) this.logger.LogInformation($"Regional Coordinator {myID}: {localCoordinatorPerSiloPerBatch.Count}");
            return Task.CompletedTask;
        }

        public override Task OnActivateAsync()
        {
            this.myID = (int)this.GetPrimaryKeyLong(out string _);
            this.expectedAcksPerBatch = new Dictionary<long, int>();
            this.bidToSubBatches = new Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>>();
            this.localCoordinatorPerSiloPerBatch = new Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>>();
            this.nonDetTxnProcessor = new NonDetTxnProcessor(myID);
            detTxnProcessor = new DetTxnProcessor(
                this.logger,
                this.GrainReference,
                this.myID,
                this.expectedAcksPerBatch,
                this.bidToSubBatches,
                this.localCoordinatorPerSiloPerBatch);
            return base.OnActivateAsync();
        }


        // for PACT
        public async Task<Tuple<TransactionRegistInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>>> NewTransaction(List<Tuple<int, string>> siloList)
        {
            this.GetPrimaryKeyLong(out string region);
            this.logger.LogInformation($"[{region}] calling detTxnProcessor.NewDet(siloList)");
            var id = await detTxnProcessor.NewDet(siloList);
            Debug.Assert(this.localCoordinatorPerSiloPerBatch.ContainsKey(id.Item1));
            this.logger.LogInformation($"[{region}] returning transaction registration info");
            var info = new TransactionRegistInfo(id.Item1, id.Item2, detTxnProcessor.highestCommittedBid);  // bid, tid, highest committed bid
            return new Tuple<TransactionRegistInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>>(info, this.localCoordinatorPerSiloPerBatch[id.Item1]);
        }

        // for ACT
        public async Task<TransactionRegistInfo> NewTransaction()
        {
            var tid = await nonDetTxnProcessor.NewNonDet();
            return new TransactionRegistInfo(tid, detTxnProcessor.highestCommittedBid);
        }

        public Task PassToken(BasicToken token)
        {
            long curBatchID = -1;
            var elapsedTime = (DateTime.Now - timeOfBatchGeneration).TotalMilliseconds;
            if (elapsedTime >= batchSizeInMSecs)
            {
                curBatchID = detTxnProcessor.GenerateBatch(token);
                if (curBatchID != -1) timeOfBatchGeneration = DateTime.Now;
            }

            nonDetTxnProcessor.EmitNonDetTransactions(token);

            if (detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
                token.highestCommittedBid = detTxnProcessor.highestCommittedBid;
            else detTxnProcessor.highestCommittedBid = token.highestCommittedBid;

            _ = neighborCoord.PassToken(token);
            if (curBatchID != -1) _ = EmitBatch(curBatchID);
            return Task.CompletedTask;
        }

        async Task EmitBatch(long bid)
        {
            var id = this.GetPrimaryKeyLong(out string region);
            this.logger.LogInformation($"[{region}] regional coordinator with id:[{id}] is going to emit batch");
            var curScheduleMap = this.bidToSubBatches[bid];

            var coords = this.localCoordinatorPerSiloPerBatch[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var localCoordinatorID = localCoordID.Item1;
                var localCoordinatorRegionAndServer = localCoordID.Item2;
                this.logger.LogInformation($"[{region}] regional coordinator trying to emit batch to {localCoordinatorRegionAndServer} with id: {localCoordinatorID}");
                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorID, localCoordinatorRegionAndServer);
                _ = dest.ReceiveBatchSchedule(item.Value);
            }
        }

        public async Task AckBatchCompletion(long bid)
        {
            // count down the number of expected ACKs from different silos
            this.expectedAcksPerBatch[bid]--;
            if (this.expectedAcksPerBatch[bid] != 0) 
            {
                return;
            }

            // commit the batch
            await detTxnProcessor.WaitPrevBatchToCommit(bid);
            detTxnProcessor.AckBatchCommit(bid);

            // send ACKs to local coordinators
            var curScheduleMap = this.bidToSubBatches[bid];
            var coords = this.localCoordinatorPerSiloPerBatch[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];

                var localCoordinatorID = localCoordID.Item1;
                var localCoordinatorRegionAndServer = localCoordID.Item2;

                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorID, localCoordinatorRegionAndServer);
                _ = dest.AckGlobalBatchCommit(bid);
            }

            // garbage collection
            this.bidToSubBatches.Remove(bid);
            this.localCoordinatorPerSiloPerBatch.Remove(bid);
            this.expectedAcksPerBatch.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            await detTxnProcessor.WaitBatchCommit(bid);
        }

        public Task SpawnGlobalCoordGrain(IRegionalCoordinatorGrain neighbor)
        {
            this.detTxnProcessor.Init();
            this.nonDetTxnProcessor.Init();

            this.neighborCoord = neighbor;

            this.batchSizeInMSecs = Constants.batchSizeInMSecsBasic;
            for (int i = Constants.numSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeed;
            this.timeOfBatchGeneration = DateTime.Now;

            return Task.CompletedTask;
        }
    }
}