using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class RegionalConfigGrainPlacement : IPlacementDirector
    {
        private readonly ILogger logger;
        private readonly RegionalSilosPlacementInfo regionalSilos;

        public RegionalConfigGrainPlacement(ILogger logger, RegionalSilosPlacementInfo regionalSilos)
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
            this.logger.LogWarning($"The string key of the grain {region} was not found in the RegionalSilos dictionary");

            // TODO: Handle this in a better way.
            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            return Task.FromResult(silos[0]);
        }
    }

    [Serializable]
    public class RegionalConfigGrainPlacementStrategy : PlacementStrategy
    {

    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RegionalConfigGrainPlacementStrategyAttribute : PlacementAttribute

    {
        public RegionalConfigGrainPlacementStrategyAttribute() : base(new RegionalConfigGrainPlacementStrategy())
        {
        }
    }
}