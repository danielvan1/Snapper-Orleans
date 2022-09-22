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

namespace Concurrency.Implementation.Configuration
{
    [RegionalConfigurationGrainPlacementStrategy]
    public class RegionalConfigurationGrain : Grain, IRegionalConfigGrain
    {
        private readonly RegionalConfiguration regionalConfiguration;
        private readonly ILogger<RegionalConfiguration> logger;
        private bool tokenEnabled;

        public RegionalConfigurationGrain(ILogger<RegionalConfiguration> logger, RegionalConfiguration regionalConfiguration)
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
            this.logger.LogInformation("InitializeRegionalCoordinators in region {currentRegion}", this.GrainReference, currentRegion);

            if (!this.regionalConfiguration.NumberOfSilosInRegion.TryGetValue(currentRegion, out int silos))
            {
                this.logger.LogError("Could not find number of silos in the region {currentRegion}", this.GrainReference, currentRegion);

                return;
            }

            var initRegionalCoordinatorTasks = new List<Task>();

            // Connecting last coordinator with the first, so making the ring of coordinators circular.
            var coordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(silos - 1, currentRegion);
            var nextCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(0, currentRegion);
            initRegionalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));

            for (int i = 0; i < silos - 1; i++)
            {
                coordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(i, currentRegion);
                nextCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(i + 1, currentRegion);

                initRegionalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));
            }

            await Task.WhenAll(initRegionalCoordinatorTasks);

            this.logger.LogInformation("Initialized all regional coordinators in region {currentRegion}", this.GrainReference, currentRegion);

            if (!this.tokenEnabled)
            {
                var coordinator0 = GrainFactory.GetGrain<IRegionalCoordinatorGrain>(0, currentRegion);
                BasicToken token = new BasicToken();
                await coordinator0.PassToken(token);
                this.tokenEnabled = true;
            }
        }
    }
}