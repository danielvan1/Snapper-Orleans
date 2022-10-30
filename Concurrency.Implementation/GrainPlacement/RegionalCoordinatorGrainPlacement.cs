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
        private readonly RegionalSiloPlacementInfo regionalPlacementInfo;

        public RegionalCoordinatorGrainPlacement(ILogger<RegionalCoordinatorGrainPlacement> logger, RegionalSiloPlacementInfo regionalPlacementInfo)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.regionalPlacementInfo = regionalPlacementInfo ?? throw new ArgumentNullException(nameof(regionalPlacementInfo));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            IList<SiloAddress> compatibleSilos = context.GetCompatibleSilos(target);

            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string siloId);

            this.logger.LogInformation("RegionalCoordinator CompataibleSilos: {silos}", context.GetCompatibleSilos(target));
            this.logger.LogInformation("RegionalCoordinator: CurrentRegion: {region} ---- dict: {dict}", siloId, string.Join(", ", this.regionalPlacementInfo.RegionsSiloInfo.Select(kv => kv.Key)));

            if (this.regionalPlacementInfo.RegionsSiloInfo.TryGetValue(siloId, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.IPEndPoint.Address) &&
                                                                       siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                                 .First();

                this.logger.LogInformation("RegionalCoordinator: Chosen siloAddress: {ad}-{port} --- The siloId {siloId} and SiloInfo: {ad}-{port}", siloAddress.Endpoint.Address, siloAddress.Endpoint.Port, siloId, siloInfo.IPEndPoint.Address, siloInfo.SiloPort);

                return Task.FromResult(siloAddress);
            }

            this.logger.LogError("Can not find the correct Silo for {nameof(LocalCoordinatorGrain)}. The given region is {region}", nameof(RegionalCoordinatorGrain), siloId );

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