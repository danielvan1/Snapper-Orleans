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
            Console.WriteLine(string.Join(", ", silos));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            // long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);
            string region = "EU";

            if(this.silos.TryGetValue(region, out SiloInfo siloInfo))
            {
                Console.WriteLine($"siloInfo: {siloInfo}");
                Console.WriteLine("HerPDerp2");
                Console.WriteLine(string.Join(" ,", context.GetCompatibleSilos(target)));
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Port == 15001)
                                                 .First();   


                return Task.FromResult(siloAddress);
            }

            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            return Task.FromResult(silos[0]);
            // TODO: Handle this in a better way.
        }
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