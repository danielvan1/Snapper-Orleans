using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class RegionalCoordinatorGrainPlacement : IPlacementDirector
    {
        private readonly Dictionary<string, SiloInfo> silos;

        public RegionalCoordinatorGrainPlacement(Dictionary<string, SiloInfo> silos)
        {
            this.silos = silos ?? throw new ArgumentNullException(nameof(silos));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if(this.silos.TryGetValue(region, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target).Where(siloAddress => siloAddress.Endpoint.Equals(siloInfo.ipEndPoint)).First();   

                return Task.FromResult(siloAddress);
            }

            // TODO: Handle this in a better way.
            throw new Exception("HerpDerp");
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