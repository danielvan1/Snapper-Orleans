using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Configuration
{
    [LocalConfigurationGrainPlacementStrategy]
    public class LocalConfigurationGrain : Grain, ILocalConfigGrain
    {
        private readonly ILogger<LocalConfigurationGrain> logger;
        private readonly LocalConfiguration localConfiguration;

        public LocalConfigurationGrain(ILogger<LocalConfigurationGrain> logger, LocalConfiguration localConfiguration)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localConfiguration = localConfiguration ?? throw new ArgumentNullException(nameof(localConfiguration));
        }

        public async Task InitializeLocalCoordinators(string currentRegion)
        {
            this.logger.LogInformation("Initializing configuration in local config grain in region: {currentRegion}", this.GrainReference, currentRegion);
            if (!this.localConfiguration.SiloKeysPerRegion.TryGetValue(currentRegion, out List<string> siloKeys))
            {
                this.logger.LogError("Currentregion: {currentRegion} does not exist in the dictionary", this.GrainReference, currentRegion);

                return;
            }

            var initializeLocalCoordinatorsTasks = new List<Task>();

            // regionAndServerKey should be similar to EU-EU-1
            // which indicate: <deployed region>-<home region>-<server id>
            foreach (string regionAndServerKey in siloKeys)
            {
                var coordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(
                    Constants.NumberOfLocalCoordinatorsPerSilo - 1, regionAndServerKey);
                var nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(0, regionAndServerKey);
                initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));

                for (int i = 0; i < Constants.NumberOfLocalCoordinatorsPerSilo - 1; i++)
                {
                    coordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(i, regionAndServerKey);
                    nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(i + 1, regionAndServerKey);
                    initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));
                }
            }

            await Task.WhenAll(initializeLocalCoordinatorsTasks);

            this.logger.LogInformation("Spawned all local coordinators in region {currentRegion}", this.GrainReference, currentRegion);

            var passInitialTokenTasks = new List<Task>();
            // Wait until all of the local coordinators has started
            // Then pass the first coordinator in the chain the first token
            foreach (string regionAndServerKey in siloKeys)
            {
                passInitialTokenTasks.Add(this.PassInitialToken(regionAndServerKey));
            }

            await Task.WhenAll(passInitialTokenTasks);

            this.logger.LogInformation("Passed the initial token for local coordinators in region {currentRegion}", this.GrainReference, currentRegion);
        }

        // Start the circular token passing by sending the initial token to
        // the first coordinator in the chain, the first coordinator
        // will then pass it to the second until it wraps around to the
        // first again and it will continue forever
        private Task PassInitialToken(string regionAndServer)
        {
            int firstCoordinatorInChain = 0;
            var coordinator0 = GrainFactory.GetGrain<ILocalCoordinatorGrain>(
                firstCoordinatorInChain,
                regionAndServer);
            LocalToken token = new LocalToken();

            return coordinator0.PassToken(token);
        }
    }
}