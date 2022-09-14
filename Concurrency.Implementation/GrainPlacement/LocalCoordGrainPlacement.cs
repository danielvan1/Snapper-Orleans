using System;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.GrainPlacement
{
    public class LocalCoordGrainPlacement : IPlacementDirector
    {
        private readonly Dictionary<string, SiloInfo> replicas;
        private readonly SiloInfo siloInfo;

        public LocalCoordGrainPlacement(Dictionary<string, SiloInfo> replicas)
        {
            this.replicas = replicas ?? throw new ArgumentNullException(nameof(replicas));
        } 

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            SiloAddress[] silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();

            var siloId = 0;

            var grainID = (int)target.GrainIdentity.GetPrimaryKeyLong(out string region);

            if (this.replicas.TryGetValue(region, out SiloInfo replica)) 
            {
                siloId = replica.SiloId;
            }
            else
            {
                Console.WriteLine("Could not find the correct siloKey to spawn the grain!!");
            }

            return Task.FromResult(silos[siloId]);
        }
    }

    [Serializable]
    public class LocalCoordGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalCoordGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalCoordGrainPlacementStrategyAttribute() : base(new LocalCoordGrainPlacementStrategy())
        {
        }
    }
}