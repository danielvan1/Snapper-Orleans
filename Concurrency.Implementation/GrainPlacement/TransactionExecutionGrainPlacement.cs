using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Exceptions;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class TransactionExecutionGrainPlacement : IPlacementDirector
    {
        private readonly ILogger<TransactionExecutionGrainPlacement> logger;
        private readonly LocalSiloPlacementInfo localSiloPlacementInfo;

        public TransactionExecutionGrainPlacement(ILogger<TransactionExecutionGrainPlacement> logger, LocalSiloPlacementInfo localSiloPlacementInfo)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localSiloPlacementInfo = localSiloPlacementInfo ?? throw new ArgumentNullException(nameof(localSiloPlacementInfo));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string siloId);

            if (this.localSiloPlacementInfo.LocalSiloInfo.TryGetValue(siloId, out SiloInfo siloInfo))
            {
                // SiloAddress siloAddress = context.GetCompatibleSilos(target)
                //                                  .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.IPEndPoint.Address) &&
                //                                                        siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                //                                  .First();

                var siloAddress = SiloAddress.New(siloInfo.IPEndPoint, 0);
                return Task.FromResult(siloAddress);
            }

            throw new GrainPlacementException($"Wrong placement of grain");
        }
    }

    [Serializable]
    public class TransactionExecutionGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class TransactionExecutionGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public TransactionExecutionGrainPlacementStrategyAttribute() : base(new TransactionExecutionGrainPlacementStrategy())
        {
        }
    }
}
