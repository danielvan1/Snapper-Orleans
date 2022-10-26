using System;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using System.Linq;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalConfigurationGrainPlacement : IPlacementDirector
    {
        private readonly SiloInfo siloInfo;

        public GlobalConfigurationGrainPlacement(SiloInfo siloInfo)
        {
            this.siloInfo = siloInfo ?? throw new ArgumentNullException(nameof(siloInfo));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target);   // get the list of registered silo hosts

            SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                             .Where(siloAddress => siloAddress.Endpoint.Address.Equals(siloInfo.IPEndPoint.Address) &&
                                                                   siloAddress.Endpoint.Port.Equals(siloInfo.SiloPort))
                                             .First();

            return Task.FromResult(siloAddress);
        }
    }

    [Serializable]
    public class GlobalConfigurationGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GlobalConfigurationGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GlobalConfigurationGrainPlacementStrategyAttribute() : base(new GlobalConfigurationGrainPlacementStrategy())
        {
        }
    }
}