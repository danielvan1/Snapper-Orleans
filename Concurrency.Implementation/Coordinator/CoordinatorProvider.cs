using System;
using Concurrency.Interface.Coordinator;
using Microsoft.Extensions.Logging;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class CoordinatorProvider : ICoordinatorProvider
    {
        private readonly Random random;
        private readonly ILogger<CoordinatorProvider> logger;

        public CoordinatorProvider(ILogger<CoordinatorProvider> logger)
        {
            this.random = new Random();
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public ILocalCoordinatorGrain GetLocalCoordinatorGrain(int id, string region, IGrainFactory grainFactory)
        {
            int localCoordinatorId = id % Constants.NumberOfLocalCoordinatorsPerSilo;
            this.logger.LogInformation("Creating local coordinator with id: {id} and region: {region}", localCoordinatorId, region);

            return grainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorId, region);
        }

        // TODO: Add a way to get the number of regionalCoordinators.
        public IRegionalCoordinatorGrain GetRegionalCoordinator(int id, string regionId, IGrainFactory grainFactory)
        {
            string region = regionId.Substring(0, 2);
            int regionalCoordinatorId = 0;
            this.logger.LogInformation("Creating regional coordinator with id: {id} and region: {region}", regionalCoordinatorId, region);

            return grainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordinatorId, region);
        }
    }
}