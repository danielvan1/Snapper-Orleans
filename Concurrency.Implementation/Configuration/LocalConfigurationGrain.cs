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
        private readonly ILogger logger;
        private readonly LocalConfiguration localConfiguration;
        private bool tokenEnabled;

        public LocalConfigurationGrain(ILogger logger, LocalConfiguration localConfiguration)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localConfiguration = localConfiguration ?? throw new ArgumentNullException(nameof(localConfiguration));
        }

        public override Task OnActivateAsync()
        {
            this.tokenEnabled = false;

            return base.OnActivateAsync();
        }

        public async Task InitializeLocalCoordinators(string currentRegion)
        {
            this.logger.LogInformation($"Initializing configuration in local config grain in region: {currentRegion}");
            if(!this.localConfiguration.SiloKeysPerRegion.TryGetValue(currentRegion, out List<string> siloKeys))
            {
                this.logger.LogError($"Currentregion: {currentRegion} does not exist in the dictionary");

                return;
            }

            var initializeLocalCoordinatorsTasks = new List<Task>();

            // SiloKey should be similar to EU-EU-x
            foreach(string siloKey in siloKeys)
            {
                var coordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(Constants.NumberOfLocalCoordinatorsPerSilo - 1, siloKey);
                var nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(0, siloKey);
                initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));

                for(int i = 0; i < Constants.NumberOfLocalCoordinatorsPerSilo - 1; i++)
                {
                    coordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(i, siloKey);
                    nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordinatorGrain>(i + 1, siloKey);

                    initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));
                }

            }

            await Task.WhenAll(initializeLocalCoordinatorsTasks);

            // This logic has to be after we start the coordinators, otherwise the passToken method
            // could be called before this chain is fully initialized and the <nextCoordinator> in
            // nextCoordinator.passToken(<token>) will be null.
            if (!this.tokenEnabled)
            {
                var coordinator0 = GrainFactory.GetGrain<ILocalCoordinatorGrain>(0, siloKeys[0]);
                LocalToken token = new LocalToken();
                await coordinator0.PassToken(token);
                this.tokenEnabled = true;
            }

            this.logger.LogInformation($"Spawned all local coordinators in region {currentRegion}");
        }
    }
}