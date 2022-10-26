using System;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalCoordinatorGrainPlacement : IPlacementDirector
    {
        private readonly SiloInfo siloInfo;

        public GlobalCoordinatorGrainPlacement(SiloInfo siloInfo)
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
    public class GlobalCoordinatorGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GlobalCoordinatorGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GlobalCoordinatorGrainPlacementStrategyAttribute() : base(new GlobalCoordinatorGrainPlacementStrategy())
        {
        }
    }
}