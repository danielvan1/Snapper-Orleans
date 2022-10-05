using System;
using Concurrency.Implementation.LoadBalancing;
using Concurrency.Interface.Coordinator;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class CoordinatorProvider : ICoordinatorProvider
    {
        private readonly Random random;

        public CoordinatorProvider()
        {
            this.random = new Random();
        }

        public ILocalCoordinatorGrain GetLocalCoordinatorGrain(int id, string region, IGrainFactory grainFactory)
        {
            return grainFactory.GetGrain<ILocalCoordinatorGrain>(id % Constants.NumberOfLocalCoordinatorsPerSilo, region);
        }

        public IRegionalCoordinatorGrain GetRegionalCoordinator(int id, string region, IGrainFactory grainFactory)
        {
            return grainFactory.GetGrain<IRegionalCoordinatorGrain>(0, region.Substring(0,2));
        }
    }
}