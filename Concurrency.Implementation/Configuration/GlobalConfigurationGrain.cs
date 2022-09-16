using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Configuration
{
    [GlobalConfigurationGrainPlacementStrategy]
    public class GlobalConfigurationGrain : Grain, IGlobalConfigurationGrain
    {
        private readonly ILoggerGroup loggerGroup;  // this logger group is only accessible within this silo host
        private readonly ICoordMap coordMap;
        private readonly GlobalConfiguration globalConfiguration;
        private bool tokenEnabled;
        private ILocalConfigGrain[] configGrains;

        public GlobalConfigurationGrain(ILoggerGroup loggerGroup, ICoordMap coordMap, 
                                        GlobalConfiguration globalConfiguration)   // dependency injection
        {
            this.loggerGroup = loggerGroup ?? throw new System.ArgumentNullException(nameof(loggerGroup));
            this.coordMap = coordMap ?? throw new System.ArgumentNullException(nameof(coordMap));
            this.globalConfiguration = globalConfiguration ?? throw new System.ArgumentNullException(nameof(globalConfiguration));

            // create the log folder if not exists
            if (!Directory.Exists(Constants.logPath))
            {
                Directory.CreateDirectory(Constants.logPath);
            }
        }

        public override Task OnActivateAsync()
        {
            this.tokenEnabled = false;

            return base.OnActivateAsync();
        }

        public async Task InitializeGlobalCoordinators()
        {
            // initialize global coordinators
            Console.WriteLine("Initializing global coordinators");
            IReadOnlyList<string> regions = this.globalConfiguration.Regions;
            Console.WriteLine($"The given regions are: {string.Join(", ", regions)}");

            var initGlobalCoordinatorTasks = new List<Task>();

            // Connecting last coordinator with the first, so making the ring of coordinators circular.
            var coordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(regions.Count - 1, regions[regions.Count - 1]);
            var nextCoordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(0, regions[0]);
            initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));

            for (int i = 0; i < regions.Count - 1; i++)
            {
                string region = regions[i];
                string nextRegion = regions[i + 1];

                coordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(i, region);
                nextCoordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(i + 1, nextRegion);

                initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));
            }

            await Task.WhenAll(initGlobalCoordinatorTasks);

            Console.WriteLine("Initialized all global coordinators");

            if (!this.tokenEnabled)
            {
                var coordinator0 = GrainFactory.GetGrain<IGlobalCoordGrain>(0, regions[0]);
                BasicToken token = new BasicToken();
                await coordinator0.PassToken(token);
                this.tokenEnabled = true;
            }
        }
    }
}