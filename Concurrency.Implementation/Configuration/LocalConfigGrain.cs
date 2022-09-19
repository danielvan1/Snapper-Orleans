using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Microsoft.Extensions.Logging;
using Orleans;

namespace Concurrency.Implementation.Configuration
{
    [LocalConfigGrainPlacementStrategy]
    public class LocalConfigGrain : Grain, ILocalConfigGrain
    {
        private const int NumberOfLocalCoordinatorsPerSilo = 4;
        private readonly ILogger logger;
        private readonly LocalConfiguration localConfiguration;
        private bool tokenEnabled;

        public LocalConfigGrain(ILogger logger, LocalConfiguration localConfiguration)
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
            this.logger.LogInformation("Herpderp123123n12i3n12");

            foreach(string siloKey in siloKeys)
            {
                var coordinator = this.GrainFactory.GetGrain<ILocalCoordGrain>(NumberOfLocalCoordinatorsPerSilo - 1, siloKey);
                var nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordGrain>(0, siloKey);
                initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));

                for(int i = 0; i < NumberOfLocalCoordinatorsPerSilo; i++)
                {
                    coordinator = this.GrainFactory.GetGrain<ILocalCoordGrain>(i, siloKey);
                    nextCoordinator = this.GrainFactory.GetGrain<ILocalCoordGrain>(i + 1, siloKey);

                    initializeLocalCoordinatorsTasks.Add(coordinator.SpawnLocalCoordGrain(nextCoordinator));
                }
            }

            await Task.WhenAll(initializeLocalCoordinatorsTasks);

            this.logger.LogInformation("Spawned all local coordinators");
        }
    }
}