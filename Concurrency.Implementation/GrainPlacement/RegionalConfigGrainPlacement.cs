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
            long configGrainId = target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if(this.silos.TryGetValue(region, out SiloInfo siloInfo))
            {
                Console.WriteLine($"siloInfo: {siloInfo}");
                Console.WriteLine("HerPDerp2");
                foreach (var siloAddress2 in context.GetCompatibleSilos(target))
                {
                    Console.WriteLine("----");
                    Console.WriteLine(siloAddress2.Endpoint.Address.ToString() + siloAddress2.Endpoint.Port.ToString());
                    Console.WriteLine(siloInfo.ipEndPoint.Address.ToString() + siloInfo.SiloPort.ToString());
                    Console.WriteLine("----");
                }
                SiloAddress siloAddress = context.GetCompatibleSilos(target)
                                                 .Where(siloAddress => siloAddress.Endpoint.Address.ToString() == siloInfo.ipEndPoint.Address.ToString() &&
                                                                       siloAddress.Endpoint.Port == siloInfo.SiloPort)
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