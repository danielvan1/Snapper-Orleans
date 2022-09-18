using System;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Orleans;

namespace Concurrency.Implementation.Configuration
{
    [RegionalConfigGrainPlacementStrategy]
    public class RegionalConfigGrain : Grain, IRegionalConfigGrain
    {
        private readonly RegionalConfiguration regionalConfiguration;

        public RegionalConfigGrain(RegionalConfiguration regionalConfiguration)
        {
            Console.WriteLine($"Initializing regional coordinator Reached here");
            this.regionalConfiguration = regionalConfiguration ?? throw new ArgumentNullException(nameof(regionalConfiguration));
        }

        public async Task InitializeRegionalCoordinators(string currentRegion)
        {
            Console.WriteLine($"Initializing regional coordinators {currentRegion}");

            // if(this.regionalConfiguration.NumberOfSilosInRegion.TryGetValue(currentRegion, out int silos))
            // {

            // }


            // var initGlobalCoordinatorTasks = new List<Task>();

            // Connecting last coordinator with the first, so making the ring of coordinators circular.
            // var coordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(regions.Count - 1, regions[regions.Count - 1]);
            // var nextCoordinator = this.GrainFactory.GetGrain<IGlobalCoordGrain>(0, regions[0]);
            // initGlobalCoordinatorTasks.Add(coordinator.SpawnGlobalCoordGrain(nextCoordinator));

            // for (int i = 0; i < regions.Count - 1; i++)
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