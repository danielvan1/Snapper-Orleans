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
    public class GlobalConfigGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {

            // globalConfigGrainID here is just used to check if its ID = 0 (for some reason)
            var globalConfigGrainID = (int)target.GrainIdentity.PrimaryKeyLong;
            Debug.Assert(globalConfigGrainID == 0);     // there is only one global config grain in the whole system

            var silos = context.GetCompatibleSilos(target).Where(silo => silo.Endpoint.Port == 15000).ToArray();   // get the list of registered silo hosts
            Debug.Assert(silos.Length == 0);     // there is only one global config grain in the whole system
            var silo = 0;
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class GlobalConfigGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GlobalConfigGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GlobalConfigGrainPlacementStrategyAttribute() : base(new GlobalConfigGrainPlacementStrategy())
        {
        }
    }
}