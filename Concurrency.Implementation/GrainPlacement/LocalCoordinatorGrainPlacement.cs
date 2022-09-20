using System;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.Exceptions;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class LocalCoordinatorGrainPlacement : IPlacementDirector
    {
        private readonly ILogger logger;
        private readonly LocalSiloPlacementInfo localSiloPlacementInfo;

        public LocalCoordinatorGrainPlacement(ILogger logger, LocalSiloPlacementInfo localSiloPlacementInfo)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localSiloPlacementInfo = localSiloPlacementInfo ?? throw new ArgumentNullException(nameof(localSiloPlacementInfo));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if (this.localSiloPlacementInfo.LocalSiloInfo.TryGetValue(region, out SiloInfo siloInfo))
            {
                // TODO: Why is this sequence sometimes empty? Race condition?
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.ipEndPoint.Address) &&
                                                                       siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                                 .First();

                return Task.FromResult(siloAddress);
            }

            this.logger.LogError($"Local coordinator ID: {region}");
            this.logger.LogError(string.Join(", ", this.localSiloPlacementInfo.LocalSiloInfo));

            throw new GrainPlacementException($"Wrong placement of {nameof(LocalCoordinatorGrain)}");
            // TODO: Handle this in a better way.
            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            return Task.FromResult(silos[0]);
        }
    }

    [Serializable]
    public class LocalCoordinatorGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalCoordinatorGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalCoordinatorGrainPlacementStrategyAttribute() : base(new LocalCoordinatorGrainPlacementStrategy())
        {
        }
    }
}