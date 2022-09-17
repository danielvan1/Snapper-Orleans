using System;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalConfigurationGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target);   // get the list of registered silo hosts
            Console.WriteLine($"silos: {string.Join(", ", silos)}");

            return Task.FromResult(silos[0]);
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