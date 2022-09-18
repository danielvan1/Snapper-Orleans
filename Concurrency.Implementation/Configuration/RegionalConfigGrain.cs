using System;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;

namespace Concurrency.Implementation.Configuration
{
    [RegionalConfigGrainPlacementStrategy]
    public class RegionalConfigGrain : Grain, IRegionalConfigGrain
    {
        private readonly RegionalConfiguration regionalConfiguration;
        private readonly ILogger logger;

        public RegionalConfigGrain(ILogger logger, RegionalConfiguration regionalConfiguration)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.regionalConfiguration = regionalConfiguration ?? throw new ArgumentNullException(nameof(regionalConfiguration));
        }

        public async Task InitializeRegionalCoordinators(string currentRegion)
        {
            this.logger.LogInformation(0, null, $"InitializeRegionalCoordinators in region {currentRegion}", null);

            if (!this.regionalConfiguration.NumberOfSilosInRegion.TryGetValue(currentRegion, out int silos))
            {
                this.logger.LogError($"Could not find number of silos in the region {currentRegion}");

                return;
            }


            // var initGlobalCoordinatorTasks = new List<Task>();

            // // Connecting last coordinator with the first, so making the ring of coordinators circular.
            // var coordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(silos - 1, regions[silos - 1]);
            // var nextCoordinator = this.GrainFactory.GetGrain<IRegionalCoordinatorGrain>(0, regions[0]);
            // initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));

            // for (int i = 0; i < silos; i++)
            // {
            //     string region = regions[i];
            //     string nextRegion = regions[i + 1];

            //     coordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(i, region);
            //     nextCoordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(i + 1, nextRegion);

            //     initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));
            // }

            // await Task.WhenAll(initGlobalCoordinatorTasks);

            // Console.WriteLine("Initialized all global coordinators");

            // if (!this.tokenEnabled)
            // {
            //     var coordinator0 = GrainFactory.GetGrain<IGlobalCoordGrain>(0, regions[0]);
            //     BasicToken token = new BasicToken();
            //     await coordinator0.PassToken(token);
            //     this.tokenEnabled = true;
            // }


        }
    }
}