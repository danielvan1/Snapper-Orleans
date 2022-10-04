using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator.Regional;
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

            var numberOfSilosInTestCluster = 2;
            IList<SiloAddress> compatibleSilos = context.GetCompatibleSilos(target);
            bool isTestSilo = compatibleSilos.Count == numberOfSilosInTestCluster;
            if (isTestSilo) {
                this.logger.Info("Is using test placement strategy");
                return Task.FromResult(compatibleSilos[0]);
            }

            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if (this.regionalSilos.RegionsSiloInfo.TryGetValue(region, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.ipEndPoint.Address) &&
                                                                       siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                                 .First();

                return Task.FromResult(siloAddress);
            }

            this.logger.LogError("Can not find the correct Silo for {nameof(LocalCoordinatorGrain)}. The given region is {region}", nameof(RegionalCoordinatorGrain), region );

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