using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using System.Diagnostics;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalCoordGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).Where(silo => silo.Endpoint.Port == 15000).ToArray();
            var silo = 0;
            Debug.Assert(silos.Length == 1);
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class GlobalCoordGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GlobalCoordGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GlobalCoordGrainPlacementStrategyAttribute() : base(new GlobalCoordGrainPlacementStrategy())
        {
        }
    }
}