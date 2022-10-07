using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public class TransactionBroadCaster : ITransactionBroadCaster
    {
        private readonly ILogger<TransactionBroadCaster> logger;
        private readonly IGrainFactory grainFactory;
        private readonly List<string> regions;

        public TransactionBroadCaster(ILogger<TransactionBroadCaster> logger, IGrainFactory grainFactory, List<string> regions)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
            this.regions = regions ?? throw new ArgumentNullException(nameof(regions));
        }

        public Task StartTransactionInAllOtherRegions(string firstFunction, object functionInput, List<GrainAccessInfo> grainAccessInfos)
        {
            this.logger.LogInformation("BroadCasting transaction to all other regions. The grainaccessinfos are: {infos}", string.Join(", ", grainAccessInfos));

            foreach (GrainAccessInfo grainAccessInfo in grainAccessInfos)
            {
                long id = grainAccessInfo.Id;

                List<GrainAccessInfo> newGrainAccessInfo = new List<GrainAccessInfo>();

                foreach (string region in this.regions)
                {
                    if(region.Equals(grainAccessInfo.GetDeploymentRegion())) continue;

                    string replicaRegion = grainAccessInfo.ReplaceDeploymentRegion(region);

                    newGrainAccessInfo.Add(new GrainAccessInfo()
                    {
                        Id = grainAccessInfo.Id,
                        Region = replicaRegion,
                        GrainClassName = grainAccessInfo.GrainClassName
                    });
                }

                foreach (string region in this.regions)
                {
                    if(region.Equals(grainAccessInfo.GetDeploymentRegion())) continue;
                    string replicaRegion = grainAccessInfo.ReplaceDeploymentRegion(region);

                    var transactionExecutionGrain = this.grainFactory.GetGrain<ITransactionExecutionGrain>(grainAccessInfo.Id, replicaRegion, grainAccessInfo.GrainClassName);
                    transactionExecutionGrain.StartReplicaTransaction(firstFunction, functionInput, newGrainAccessInfo);
                }
            }

            this.logger.LogInformation("Finished broadCasting transaction to all other regions. The grainaccessinfos are: {infos}", string.Join(", ", grainAccessInfos));

            return Task.CompletedTask;
        }
    }
}