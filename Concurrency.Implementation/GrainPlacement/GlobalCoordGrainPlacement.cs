﻿using System;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalCoordGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target);   // get the list of registered silo hosts

            return Task.FromResult(silos[0]);
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