using System;
using Concurrency.Implementation.LoadBalancing;
using Orleans;

namespace Concurrency.Implementation.Coordinator
{
    public class CoordinatorProvider<T> : ICoordinatorProvider<T> where T : IGrain, IGrainWithIntegerCompoundKey
    {
        private readonly IGrainFactory grainFactory;
        private readonly Random random;

        public CoordinatorProvider(IGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
            this.random = new Random();
        }

        public T GetCoordinator(string region)
        {
            // TODO: Somehow get more information to be able to properly choose a random Coordinator
            int randomId = this.random.Next(3);

            return this.grainFactory.GetGrain<T>(randomId, region);
        }
    }
}