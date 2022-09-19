using System;
using System.Threading.Tasks;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalCoordinatorGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target);   // get the list of registered silo hosts

            return Task.FromResult(silos[0]);
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