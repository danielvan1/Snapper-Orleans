using System;
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
    public class LocalCoordGrainPlacement : IPlacementDirector
    {
        private readonly ILogger logger;
        private readonly LocalSiloPlacementInfo localSilos;

        public LocalCoordGrainPlacement(ILogger logger, LocalSiloPlacementInfo localSilos)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localSilos = localSilos ?? throw new ArgumentNullException(nameof(localSilos));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if (this.localSilos.LocalSiloInfo.TryGetValue(region, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.ipEndPoint.Address) &&
                                                                       siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                                 .First();
                this.logger.LogInformation($"Found the silo {siloAddress} for LocalCoordinatorGrain {configGrainId}-{region}");

                return Task.FromResult(siloAddress);
            }

            // TODO: Handle this in a better way.
            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            return Task.FromResult(silos[0]);
        }
    }

    [Serializable]
    public class LocalCoordGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalCoordGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalCoordGrainPlacementStrategyAttribute() : base(new LocalCoordGrainPlacementStrategy())
        {
        }
    }
}