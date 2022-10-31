using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Concurrency;
using Utilities;

namespace Concurrency.Implementation.Coordinator.Global
{
    [Reentrant]
    [GlobalCoordinatorGrainPlacementStrategy]
    public class GlobalCoordinatorGrain : Grain, IGlobalCoordinatorGrain
    {
        // coord basic info
        int myID;
        private readonly ILogger<GlobalCoordinatorGrain> logger;
        IGlobalCoordinatorGrain neighborCoord;

        // PACT
        // DetTxnProcessor detTxnProcessor;
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<string, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        Dictionary<long, Dictionary<string, int>> coordPerBatchPerSilo;        // <global bid, siloID, chosen local Coord ID>

        DateTime timeOfBatchGeneration;
        double batchSizeInMSecs;

        public override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong(out string _);
            expectedAcksPerBatch = new Dictionary<long, int>();
            bidToSubBatches = new Dictionary<long, Dictionary<string, SubBatch>>();
            coordPerBatchPerSilo = new Dictionary<long, Dictionary<string, int>>();
            return base.OnActivateAsync();
        }

        public GlobalCoordinatorGrain(ILogger<GlobalCoordinatorGrain> logger)
        {
            this.logger = logger;
        }

        public Task PassToken(TokenBase token)
        {
            long curBatchID = -1;
            var elapsedTime = (DateTime.Now - timeOfBatchGeneration).TotalMilliseconds;
            // if (elapsedTime >= batchSizeInMSecs)
            // {
            //     curBatchID = detTxnProcessor.GenerateBatch(token);
            //     if (curBatchID != -1) timeOfBatchGeneration = DateTime.Now;
            // }

            // if (detTxnProcessor.highestCommittedBid > token.HighestCommittedBid)
            //     token.HighestCommittedBid = detTxnProcessor.highestCommittedBid;
            // else detTxnProcessor.highestCommittedBid = token.HighestCommittedBid;

            // _ = neighborCoord.PassToken(token);
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

        public Task SpawnGlobalCoordGrain(IGlobalCoordinatorGrain neighbor)
        {
            // this.detTxnProcessor.Init();

            this.neighborCoord = neighbor;

            // this.loggerGroup.GetLoggingProtocol(myID, out log);

            this.batchSizeInMSecs = Constants.batchSizeInMSecsBasic;
            for (int i = Constants.numSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeed;
            this.timeOfBatchGeneration = DateTime.Now;

            return Task.CompletedTask;
        }
    }
}