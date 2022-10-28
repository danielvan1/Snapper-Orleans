using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Configuration;
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
    public class LocalConfigurationGrainPlacement : IPlacementDirector
    {
    private readonly ILogger<LocalConfigurationGrainPlacement> logger;
    private readonly RegionalSiloPlacementInfo regionalSilos;

    /// <summary>
    /// Here we use the  <see cref="RegionalSiloPlacementInfo"/> since we want to spawn each <see cref="LocalConfigurationGrain"/>  in
    /// each of the regional silos. There is one regional silo per region and this should be sufficient for
    /// spawning each <see cref="LocalCoordinatorGrain"/> in every local silo for each region.
    /// </summary>
    public LocalConfigurationGrainPlacement(ILogger<LocalConfigurationGrainPlacement> logger, RegionalSiloPlacementInfo regionalSilos)
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
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.IPEndPoint.Address) &&
                                                                       siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                                 .First();

                return Task.FromResult(siloAddress);
            }

            throw new GrainPlacementException($"Wrong placement of {nameof(LocalConfigurationGrain)}");
        }
    }

    [Serializable]
    public class LocalConfigurationGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalConfigurationGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalConfigurationGrainPlacementStrategyAttribute() : base(new LocalConfigurationGrainPlacementStrategy())
        {
        }
    }
}