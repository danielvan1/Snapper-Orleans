using System;
using System.Threading.Tasks;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class RegionalCoordinatorGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            return null;
        }
    }

    [Serializable]
    public class RegionalCoordinatorGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RegionalConfigGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public RegionalConfigGrainPlacementStrategyAttribute() : base(new RegionalCoordinatorGrainPlacementStrategy())
        {
        }
    }
}