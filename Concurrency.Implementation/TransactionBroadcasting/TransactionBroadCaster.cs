using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Implementation.TransactionExecution;
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

        public Task StartTransactionInAllOtherRegions(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfos, GrainId startGrain)
        {
            this.logger.LogInformation("BroadCasting transaction to all other regions. The grainaccessinfos are: {infos}", string.Join(", ", grainAccessInfos));

            string currentRegion = startGrain.StringId.Substring(0, 2);

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
                        Region = replicaRegion,
                        GrainClassName = grainAccessInfo.GrainClassName
                    });
                }

                var transactionExecutionGrain = this.grainFactory.GetGrain<ITransactionExecutionGrain>(startGrain.IntId, this.ReplaceDeploymentRegion(region, startGrain.StringId), startGrain.GrainClassName);

                transactionExecutionGrain.StartReplicaTransaction(firstFunction, functionInput is null ? null : this.CreateFunctionInput(region, functionInput), newGrainAccessInfo);
            }

            this.logger.LogInformation("Finished broadCasting transaction to all other regions. The grainaccessinfos are: {infos}", string.Join(", ", grainAccessInfos));

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