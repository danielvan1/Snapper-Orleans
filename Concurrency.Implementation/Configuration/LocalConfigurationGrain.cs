using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
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
        private readonly LocalCoordinatorConfiguration localConfiguration;

        public LocalConfigurationGrain(ILogger<LocalConfigurationGrain> logger, LocalCoordinatorConfiguration localConfiguration)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localConfiguration = localConfiguration ?? throw new ArgumentNullException(nameof(localConfiguration));
        }

        public async Task InitializeLocalCoordinators(string currentRegion)
        {
            this.logger.LogInformation("Initializing local coordinators in region: {currentRegion}", this.GrainReference, currentRegion);

            if (!this.localConfiguration.SiloIdPerRegion.TryGetValue(currentRegion, out List<string> siloIds))
            {
                this.logger.LogError("Currentregion: {currentRegion} does not exist in the dictionary", this.GrainReference, currentRegion);

                return;
            }

            var initializeLocalCoordinatorsTasks = new List<Task>();

            // regionAndServerKey should be similar to EU-EU-1
            // which indicate: <deployed region>-<home region>-<server id>
            foreach (string siloId in siloIds)
            {
                this.logger.LogInformation("Deploying current siloId: {siloId}", this.GrainReference, siloId);
                var coordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(Constants.NumberOfLocalCoordinatorsPerSilo - 1, siloId);
                var nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(0, siloId);
                initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));

                for (int i = 0; i < Constants.NumberOfLocalCoordinatorsPerSilo - 1; i++)
                {
                    // TODO: we might need to change the ids from coordinators and transaction execution grains since their keys overlap.
                    coordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(i, siloId);
                    nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(i + 1, siloId);
                    initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));
                }
            }

            await Task.WhenAll(initializeLocalCoordinatorsTasks);

            this.logger.LogInformation("Spawned all local coordinators in region {currentRegion}", this.GrainReference, currentRegion);

            var passInitialTokenTasks = new List<Task>();
            // Wait until all of the local coordinators has started
            // Then pass the first coordinator in the chain the first token
            foreach (string regionAndServerKey in siloIds)
            {
                passInitialTokenTasks.Add(this.PassInitialToken(regionAndServerKey));
            }

            await Task.WhenAll(passInitialTokenTasks);

            this.logger.LogInformation("Passed the initial token for local coordinators in region {currentRegion}", this.GrainReference, currentRegion);
        }

        public new virtual IGrainFactory GrainFactory
        {
            get { return base.GrainFactory; }
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