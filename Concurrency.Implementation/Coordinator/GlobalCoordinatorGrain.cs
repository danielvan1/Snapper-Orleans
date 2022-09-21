﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [GlobalCoordinatorGrainPlacementStrategy]
    public class GlobalCoordinatorGrain : Grain, IGlobalCoordinatorGrain
    {
        // coord basic info
        int myID;
        ICoordMap coordMap;
        private readonly ILogger logger;
        IGlobalCoordinatorGrain neighborCoord;

        // PACT
        DetTxnProcessor detTxnProcessor;
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<string, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        Dictionary<long, Dictionary<string, int>> coordPerBatchPerSilo;        // <global bid, siloID, chosen local Coord ID>

        // ACT
        NonDetTxnProcessor nonDetTxnProcessor;

        DateTime timeOfBatchGeneration;
        double batchSizeInMSecs;

        public override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong(out string _);
            expectedAcksPerBatch = new Dictionary<long, int>();
            bidToSubBatches = new Dictionary<long, Dictionary<string, SubBatch>>();
            coordPerBatchPerSilo = new Dictionary<long, Dictionary<string, int>>();
            nonDetTxnProcessor = new NonDetTxnProcessor(myID);
            return base.OnActivateAsync();
        }

        public GlobalCoordinatorGrain(ILogger logger)
        {
            this.logger = logger;
        }

        // for PACT
        public async Task<Tuple<TransactionRegistInfo, Dictionary<string, int>>> NewTransaction(List<Tuple<int, string>> siloList)
        {
            var id = await detTxnProcessor.NewDet(siloList);
            Debug.Assert(coordPerBatchPerSilo.ContainsKey(id.Item1));
            var info = new TransactionRegistInfo(id.Item1, id.Item2, detTxnProcessor.highestCommittedBid);  // bid, tid, highest committed bid
            return new Tuple<TransactionRegistInfo, Dictionary<string, int>>(info, coordPerBatchPerSilo[id.Item1]);
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
            var curScheduleMap = bidToSubBatches[bid];

            var coords = coordPerBatchPerSilo[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordID, "");
                _ = dest.ReceiveBatchSchedule(item.Value);
            }
        }

        public async Task AckBatchCompletion(long bid)
        {
            // count down the number of expected ACKs from different silos
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] != 0) return;

            // commit the batch
            await detTxnProcessor.WaitPrevBatchToCommit(bid);
            detTxnProcessor.AckBatchCommit(bid);

            // send ACKs to local coordinators
            var curScheduleMap = bidToSubBatches[bid];
            var coords = coordPerBatchPerSilo[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordID, "");
                _ = dest.AckGlobalBatchCommit(bid);
            }

            // garbage collection
            bidToSubBatches.Remove(bid);
            coordPerBatchPerSilo.Remove(bid);
            expectedAcksPerBatch.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            await detTxnProcessor.WaitBatchCommit(bid);
        }

        public Task SpawnGlobalCoordGrain(IGlobalCoordinatorGrain neighbor)
        {
            this.detTxnProcessor.Init();
            this.nonDetTxnProcessor.Init();

            this.neighborCoord = neighbor;

            // this.loggerGroup.GetLoggingProtocol(myID, out log);

            this.batchSizeInMSecs = Constants.batchSizeInMSecsBasic;
            for (int i = Constants.numSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeed;
            this.timeOfBatchGeneration = DateTime.Now;

            return Task.CompletedTask;
        }
    }
}