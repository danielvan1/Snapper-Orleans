using System;
using System.Collections.Generic;
using System.Reflection.Metadata;
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
    [RegionalConfigurationGrainPlacementStrategy]
    public class RegionalConfigurationGrain : Grain, IRegionalCoordinatorConfigGrain
    {
        private readonly RegionalCoordinatorConfiguration regionalConfiguration;
        private readonly ILogger<RegionalCoordinatorConfiguration> logger;
        private bool tokenEnabled;

        public RegionalConfigurationGrain(ILogger<RegionalCoordinatorConfiguration> logger, RegionalCoordinatorConfiguration regionalConfiguration)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.regionalConfiguration = regionalConfiguration ?? throw new ArgumentNullException(nameof(regionalConfiguration));
        }

        public override Task OnActivateAsync()
        {
            this.tokenEnabled = false;

            return base.OnActivateAsync();
        }

        public async Task InitializeRegionalCoordinators(string currentRegion)
        {
            this.logger.LogInformation("Going to initialize regional coordinators in region {currentRegion}", this.GrainReference, currentRegion);

            if (!this.regionalConfiguration.NumberOfSilosPerRegion.TryGetValue(currentRegion, out int silos))
            {
                this.logger.LogError("Could not find number of silos in the region {currentRegion}", this.GrainReference, currentRegion);

                return;
            }

            var initRegionalCoordinatorTasks = new List<Task>();

            // Connecting last coordinator with the first, so making the ring of coordinators circular.
            var coordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(Constants.NumberOfRegionalCoordinators - 1, currentRegion);
            var nextCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(0, currentRegion);
            initRegionalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));

            for (int i = 0; i < Constants.NumberOfRegionalCoordinators - 1; i++)
            {
                coordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(i, currentRegion);
                nextCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(i + 1, currentRegion);

                initRegionalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));
            }

            await Task.WhenAll(initRegionalCoordinatorTasks);

            this.logger.LogInformation("Initialized all regional coordinators in region {currentRegion}", this.GrainReference, currentRegion);

            if (!this.tokenEnabled)
            {
                var coordinator0 = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(0, currentRegion);
                RegionalToken token = new RegionalToken();
                await coordinator0.PassToken(token);
                this.tokenEnabled = true;

                this.logger.LogInformation("Passed the initial token for regional coordinators in region {currentRegion}", this.GrainReference, currentRegion);
            }
        }
    }
}