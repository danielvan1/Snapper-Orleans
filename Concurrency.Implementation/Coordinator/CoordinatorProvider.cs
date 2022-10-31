using System;
using Concurrency.Interface.Coordinator;
using Microsoft.Extensions.Logging;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class CoordinatorProvider : ICoordinatorProvider
    {
        private readonly ILogger<CoordinatorProvider> logger;

        public CoordinatorProvider(ILogger<CoordinatorProvider> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public ILocalCoordinatorGrain GetLocalCoordinatorGrain(int id, string siloId, IGrainFactory grainFactory)
        {
            int localCoordinatorId = id % Constants.NumberOfLocalCoordinatorsPerSilo;
            this.logger.LogInformation("Creating local coordinator with id: {id} and region: {region}", localCoordinatorId, siloId);

            return grainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorId, siloId);
        }

        // TODO: Add a way to get the number of regionalCoordinators.
        public IRegionalCoordinatorGrain GetRegionalCoordinator(int id, string siloId, IGrainFactory grainFactory)
        {
            int regionalCoordinatorId = id % Constants.NumberOfRegionalCoordinators;
            // int regionalCoordinatorId = 0;
            this.logger.LogInformation("Creating regional coordinator with id: {id} and region: {region}", regionalCoordinatorId, siloId);

            return grainFactory.GetGrain<IRegionalCoordinatorGrain>(regionalCoordinatorId, siloId);
        }
    }
}