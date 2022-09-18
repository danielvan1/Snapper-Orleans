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
    public class RegionalConfigGrainPlacement : IPlacementDirector
    {
        private readonly Dictionary<string, SiloInfo> silos;

        public RegionalConfigGrainPlacement(Dictionary<string, SiloInfo> silos)
        {
            this.silos = silos ?? throw new ArgumentNullException(nameof(silos));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if(this.silos.TryGetValue(region, out SiloInfo siloInfo))
            {
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.ToString() == siloInfo.ipEndPoint.Address.ToString() &&
                                                                       siloAddress.Endpoint.Port == siloInfo.SiloPort)
                                                 .First();   


                return Task.FromResult(siloAddress);
            }

            // TODO: Handle this in a better way.
            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            return Task.FromResult(silos[0]); }
    }

    [Serializable]
    public class RegionalConfigGrainPlacementStrategy : PlacementStrategy
    {

    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RegionalConfigGrainPlacementStrategyAttribute  : PlacementAttribute

    {
        public RegionalConfigGrainPlacementStrategyAttribute() : base(new RegionalConfigGrainPlacementStrategy())
        {
        }
    }
}