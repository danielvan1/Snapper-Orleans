using System;
using System.Collections.Generic;
using System.IO;
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
    [GlobalConfigurationGrainPlacementStrategy]
    public class GlobalConfigurationGrain : Grain, IGlobalConfigurationGrain
    {
        private readonly GlobalConfiguration globalConfiguration;
        private readonly ILogger<GlobalConfigurationGrain> logger;  // this logger group is only accessible within this silo host
        private bool tokenEnabled;

        public GlobalConfigurationGrain(ILogger<GlobalConfigurationGrain> logger, GlobalConfiguration globalConfiguration)   // dependency injection
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.globalConfiguration = globalConfiguration ?? throw new System.ArgumentNullException(nameof(globalConfiguration));

            // create the log folder if not exists
            if (!Directory.Exists(Constants.LogPath))
            {
                Directory.CreateDirectory(Constants.LogPath);
            }
        }

        public override Task OnActivateAsync()
        {
            this.tokenEnabled = false;

            return base.OnActivateAsync();
        }

        public async Task InitializeGlobalCoordinators()
        {
            this.logger.LogInformation("Initializing global coordinators", this.GrainReference);

            IReadOnlyList<string> regions = this.globalConfiguration.Regions;
            string deploymentRegion = this.globalConfiguration.DeploymentRegion;
            this.logger.LogInformation("The given regions are: {regions}", this.GrainReference, string.Join(", ", regions));

            var initGlobalCoordinatorTasks = new List<Task>();

            this.logger.LogInformation("Deploying the global coordinators in region: {region}", this.GrainReference, deploymentRegion);
            // Connecting last coordinator with the first, so making the ring of coordinators circular.
            var coordinator = this.GrainFactory.GetGrain<IGlobalCoordinatorGrain>(regions.Count - 1, deploymentRegion);
            var nextCoordinator = this.GrainFactory.GetGrain<IGlobalCoordinatorGrain>(0, deploymentRegion);
            initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));

            for (int i = 0; i < regions.Count - 1; i++)
            {
                string nextRegion = regions[i + 1];

                coordinator = this.GrainFactory.GetGrain<IGlobalCoordinatorGrain>(i, deploymentRegion);
                nextCoordinator = this.GrainFactory.GetGrain<IGlobalCoordinatorGrain>(i + 1, deploymentRegion);

                initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));
            }

            await Task.WhenAll(initGlobalCoordinatorTasks);

            this.logger.LogInformation("Initialized all global coordinators", this.GrainReference );

            if (!this.tokenEnabled)
            {
                var coordinator0 = GrainFactory.GetGrain<IGlobalCoordinatorGrain>(0, deploymentRegion);
                BasicToken token = new BasicToken();
                await coordinator0.PassToken(token);
                this.tokenEnabled = true;

                this.logger.LogInformation("Passed the initial token for global coordinators in region {deploymentRegion}", this.GrainReference, deploymentRegion);
            }

        }
    }
}