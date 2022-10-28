using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator.Replica;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public class TransactionBroadCaster : ITransactionBroadCaster
    {
        private readonly ILogger<TransactionBroadCaster> logger;
        private readonly IGrainFactory grainFactory;
        private readonly IIdHelper idHelper;
        private readonly List<string> regions;

        public TransactionBroadCaster(ILogger<TransactionBroadCaster> logger,
                                      IIdHelper IdHelper,
                                      IGrainFactory grainFactory,
                                      List<string> regions)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
            this.idHelper = IdHelper ?? throw new ArgumentNullException(nameof(IdHelper));
            this.regions = regions ?? throw new ArgumentNullException(nameof(regions));
        }

        public Task StartTransactionInAllOtherRegions(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfos, GrainId startGrain, TransactionContext transactionContext, long highestCommittedBidFromMaster)
        {
            this.logger.LogInformation("BroadCasting transaction to all other regions. The grainaccessinfos are: {infos}", string.Join(", ", grainAccessInfos));

            string currentRegion = startGrain.SiloId.Substring(0, 2);

            foreach (string region in this.regions)
            {
                if(region.Equals(currentRegion)) continue;

                List<GrainAccessInfo> newGrainAccessInfo = new List<GrainAccessInfo>();

                foreach (GrainAccessInfo grainAccessInfo in grainAccessInfos)
                {
                    long id = grainAccessInfo.Id;

                    string replicaRegion = grainAccessInfo.ReplaceDeploymentRegion(region);

                    newGrainAccessInfo.Add(new GrainAccessInfo()
                    {
                        Id = grainAccessInfo.Id,
                        SiloId = replicaRegion,
                        GrainClassNamespace = grainAccessInfo.GrainClassNamespace
                    });
                }

                var transactionExecutionGrain = this.grainFactory.GetGrain<ITransactionExecutionGrain>(startGrain.IntId, this.ReplaceDeploymentRegion(region, startGrain.SiloId), startGrain.GrainClassName);

                transactionExecutionGrain.StartReplicaTransaction(firstFunction, functionInput is null ? null : this.CreateFunctionInput(region, functionInput), newGrainAccessInfo, transactionContext, highestCommittedBidFromMaster );
            }

            this.logger.LogInformation("Finished broadCasting transaction to all other regions. The grainaccessinfos are: {infos}", string.Join(", ", grainAccessInfos));

            return Task.CompletedTask;
        }

        public Task BroadCastLocalSchedules(string currentLocalSiloId, long bid, long previousBid, Dictionary<GrainAccessInfo, LocalSubBatch> replicaSchedules)
        {
            List<string> replicaSiloIds = this.idHelper.GetLocalReplicaSiloIds(currentLocalSiloId);

            Dictionary<GrainAccessInfo, LocalSubBatch> replicaSchedulesUpdatedCoordinatorId = new Dictionary<GrainAccessInfo, LocalSubBatch>(replicaSchedules);

            foreach((_, LocalSubBatch subBatch) in replicaSchedulesUpdatedCoordinatorId)
            {
                subBatch.LocalCoordinatorId = 0;
            }

            foreach(string replicaSiloId in replicaSiloIds)
            {
                var localReplicaCoordinator = this.grainFactory.GetGrain<ILocalReplicaCoordinator>(0, replicaSiloId);

                _ = localReplicaCoordinator.ReceiveLocalSchedule(bid, previousBid, replicaSchedules);
            }

            return Task.CompletedTask;
        }

        public Task BroadCastRegionalSchedules(string currentRegionSiloId, long bid, long previousBid, Dictionary<string, SubBatch> replicaSchedules)
        {
            List<string> replicaSiloIds = this.idHelper.GetRegionalReplicaSiloIds(currentRegionSiloId);

            Dictionary<string, SubBatch> replicaSchedulesUpdatedCoordinatorId = new Dictionary<string, SubBatch>(replicaSchedules);

            foreach((_, SubBatch subBatch) in replicaSchedulesUpdatedCoordinatorId)
            {
                subBatch.LocalCoordinatorId = 0;
            }

            foreach(string replicaSiloId in replicaSiloIds)
            {
                var regionalReplicaCoordinator = this.grainFactory.GetGrain<IRegionalReplicaCoordinator>(0, replicaSiloId);

                _ = regionalReplicaCoordinator.ReceiveRegionalSchedule(bid, previousBid, replicaSchedules);
            }

            return Task.CompletedTask;
        }

        private FunctionInput CreateFunctionInput(string region, FunctionInput functionInput)
        {
            FunctionInput newFunctionInput = new FunctionInput()
            {
                DestinationGrains = new List<TransactionInfo>()
            };

            foreach(TransactionInfo transactionInfo in functionInput.DestinationGrains)
            {
                TransactionInfo newTransactionInfo = new TransactionInfo()
                {
                    DestinationGrain = transactionInfo.DestinationGrain is null ? null :  new Tuple<int, string>(transactionInfo.DestinationGrain.Item1, this.ReplaceDeploymentRegion(region, transactionInfo.DestinationGrain.Item2)),
                    Value = transactionInfo.Value
                };

                newFunctionInput.DestinationGrains.Add(newTransactionInfo);
            }

            return newFunctionInput;
        }

        private string ReplaceDeploymentRegion(string newRegion, string currentRegionId)
        {
            return $"{newRegion}-{currentRegionId.Substring(3)}";
        }
    }
}