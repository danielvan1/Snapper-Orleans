using System;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Configuration;
using Concurrency.Implementation.Coordinator;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class LocalConfigGrainPlacement : IPlacementDirector
    {
        private readonly ILogger logger;
        private readonly RegionalSilosPlacementInfo regionalSilos;

        /// <summary>
        /// Here we use the  <see cref="RegionalSilosPlacementInfo"/> since we want to spawn each <see cref="LocalConfigGrain"/>  in
        /// each of the regional silos. There is one regional silo per region and this should be sufficient for
        /// spawning each <see cref="LocalCoordGrain"/> in every local silo for each region.
        /// </summary>
        public LocalConfigGrainPlacement(ILogger logger, RegionalSilosPlacementInfo regionalSilos)
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

            this.logger.LogWarning($"The string key of the grain {region} was not found in the RegionalSilos dictionary for the local config grain");
            // TODO: Handle this in a better way.
            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();

            return Task.FromResult(silos[0]);
        }
    }

    [Serializable]
    public class LocalConfigGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalConfigGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalConfigGrainPlacementStrategyAttribute() : base(new LocalConfigGrainPlacementStrategy())
        {
        }
    }
}