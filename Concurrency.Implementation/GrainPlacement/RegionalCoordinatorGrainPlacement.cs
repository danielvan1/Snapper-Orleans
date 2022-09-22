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
    public class RegionalCoordinatorGrainPlacement : IPlacementDirector
    {
        private readonly ILogger<RegionalCoordinatorGrainPlacement> logger;
        private readonly RegionalSilosPlacementInfo regionalSilos;

        public RegionalCoordinatorGrainPlacement(ILogger<RegionalCoordinatorGrainPlacement> logger, RegionalSilosPlacementInfo regionalSilos)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.regionalSilos = regionalSilos ?? throw new ArgumentNullException(nameof(regionalSilos));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if (this.regionalSilos.RegionsSiloInfo.TryGetValue(region, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.ipEndPoint.Address) &&
                                                                       siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                                 .First();

                return Task.FromResult(siloAddress);
            }

            this.logger.LogError($"The {nameof(RegionalCoordinatorGrain)}: {configGrainId}--{region} is not found in the dictionary");
            this.logger.LogError(string.Join(", ", this.regionalSilos.RegionsSiloInfo.Keys));

            throw new GrainPlacementException($"Wrong placement of {nameof(RegionalCoordinatorGrain)}");
        }
    }

    [Serializable]
    public class RegionalCoordinatorGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RegionalCoordinatorGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public RegionalCoordinatorGrainPlacementStrategyAttribute() : base(new RegionalCoordinatorGrainPlacementStrategy())
        {
        }
    }
}