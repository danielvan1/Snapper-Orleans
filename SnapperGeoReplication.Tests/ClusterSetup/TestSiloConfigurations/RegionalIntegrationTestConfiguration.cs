using Orleans.Hosting;
using Orleans.TestingHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Serilog;
using Orleans.Runtime;
using Concurrency.Implementation.GrainPlacement;
using System;
using Orleans.Runtime.Placement;
using Concurrency.Interface.Configuration;
using System.Collections.Generic;
using Moq;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class RegionalIntegrationTestConfiguration : ISiloConfigurator, IDisposable
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddMemoryGrainStorageAsDefault();
            siloBuilder.UseInMemoryReminderService();
            var localSiloPlacementInfo = new LocalSiloPlacementInfo();
            var regionalSiloPlacementInfo = new RegionalSilosPlacementInfo();
            var numberOfSilosInRegion = new Dictionary<string, int>();
            numberOfSilosInRegion.Add("EU", 2);

            var regionalConfiguration = new RegionalConfiguration()
            {
                NumberOfSilosInRegion = numberOfSilosInRegion
            };
            var siloKeysPerRegion = new Dictionary<string, List<string>>();
            siloKeysPerRegion.Add("EU", new List<string>() {"EU-EU-0", "EU-EU-1"});
            var localConfiguration = new LocalConfiguration()
            {
                SiloKeysPerRegion = siloKeysPerRegion
            };

            this.ConfigureRegionalGrains(siloBuilder, regionalSiloPlacementInfo, regionalConfiguration, localConfiguration, localSiloPlacementInfo);
        }

        public void Dispose()
        {

        }

        private void ConfigureRegionalGrains(ISiloBuilder siloHostBuilder,
                                             RegionalSilosPlacementInfo regionalSilos,
                                             RegionalConfiguration regionalConfiguration,
                                             LocalConfiguration localConfiguration,
                                             LocalSiloPlacementInfo localSilos)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {

                Mock<ILogger> loggerMock = new Mock<ILogger>();
                serviceCollection.AddSingleton<ILogger>(loggerMock.Object);

                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(regionalConfiguration);
                serviceCollection.AddSingleton(localConfiguration);
                serviceCollection.AddSingleton(localSilos);

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalConfigurationGrainPlacementStrategy>(nameof(RegionalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigurationGrainPlacement>(typeof(RegionalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
            });
        }
    }
}