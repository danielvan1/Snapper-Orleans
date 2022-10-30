using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator.Local;
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
        private readonly ILogger<LocalCoordinatorGrainPlacement> logger;
        private readonly LocalSiloPlacementInfo localSiloPlacementInfo;

        public LocalCoordinatorGrainPlacement(ILogger<LocalCoordinatorGrainPlacement> logger, LocalSiloPlacementInfo localSiloPlacementInfo)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localSiloPlacementInfo = localSiloPlacementInfo ?? throw new ArgumentNullException(nameof(localSiloPlacementInfo));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            IList<SiloAddress> compatibleSilos = context.GetCompatibleSilos(target);

            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string siloId);

            this.logger.LogInformation("LocalCoordinator CompataibleSilos: {silos}", context.GetCompatibleSilos(target));
            this.logger.LogInformation("LocalCoordinator: CurrentRegion: {region} ---- dict: {dict}", siloId, string.Join(", ", this.localSiloPlacementInfo.LocalSiloInfo.Select(kv => kv.Key + ": " + kv.Value.IPEndPoint)));

            if (this.localSiloPlacementInfo.LocalSiloInfo.TryGetValue(siloId, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress =>
                                                 {
                                                     return siloAddress.Endpoint.Address.Equals(siloInfo.IPEndPoint.Address) &&
                                                                         siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort);
                                                 })
                                                 .First();

                this.logger.LogInformation("LocalCoordinator: Chosen siloAddress: {ad}-{port} --- The siloId {siloId} and SiloInfo: {ad}-{port}", siloAddress.Endpoint.Address, siloAddress.Endpoint.Port, siloId, siloInfo.IPEndPoint.Address, siloInfo.SiloPort);


                return Task.FromResult(siloAddress);
            }

            this.logger.LogError("Can not find the correct Silo for {nameof(LocalCoordinatorGrain)}. The given region is {region}", nameof(LocalCoordinatorGrain), siloId );

            throw new GrainPlacementException($"Wrong placement of {nameof(LocalCoordinatorGrain)}");
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