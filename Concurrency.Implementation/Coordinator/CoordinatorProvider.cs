using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
            int randomId = this.random.Next(3);

            return this.grainFactory.GetGrain<T>(randomId, region);
        }
    }
}