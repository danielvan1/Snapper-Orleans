using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.GrainPlacement
{
    public class LocalConfigGrainPlacement : IPlacementDirector
    {
        private readonly Dictionary<string, Replica> replicaSilos;
        private readonly SiloInfo currentSilo;

        public LocalConfigGrainPlacement(Dictionary<string, Replica> replicaSilos, SiloInfo currentSilo)
        {
            this.currentSilo = currentSilo ?? throw new ArgumentNullException(nameof(currentSilo));
            this.replicaSilos = replicaSilos ?? throw new ArgumentNullException(nameof(replicaSilos));
        }

        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var siloId = 0;
            if (Constants.multiSilo)
            {
                var grainID = (int)target.GrainIdentity.GetPrimaryKeyLong(out string region);
                if (this.replicaSilos.TryGetValue(region, out Replica replica)) {
                    siloId = replica.Id;
                } else
                {
                    siloId = this.currentSilo.SiloId;
                }
            }
            return Task.FromResult(silos[siloId]);
        }
    }

    [Serializable]
    public class LocalConfigGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalConfigGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalConfigGrainPlacementStrategyAttribute() : base(new LocalConfigGrainPlacementStrategy())
        {
        }
    }
}