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
using System.IO;
using Serilog.Filters;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class RegionalIntegrationTestConfiguration : ISiloConfigurator, IDisposable
    {
        private readonly string logPath = Path.Combine(Utilities.Constants.LogPath, $"Snapper-{DateTime.Now:ddMMyyyy-HHmm}.log");
        private ILogger logger;

        public void Configure(ISiloBuilder siloBuilder)
        {
            if(!Directory.Exists(Utilities.Constants.LogPath))
            {
                Directory.CreateDirectory(Utilities.Constants.LogPath);
            }

            siloBuilder.AddMemoryGrainStorageAsDefault();
            siloBuilder.UseInMemoryReminderService();
            var localSiloPlacementInfo = new LocalSiloPlacementInfo();
            var regionalSiloPlacementInfo = new RegionalSilosPlacementInfo();
            var numberOfSilosInRegion = new Dictionary<string, int>();
            numberOfSilosInRegion.Add("EU", 2);

            var regionalConfiguration = new RegionalCoordinatorConfiguration()
            {
                NumberOfSilosPerRegion = numberOfSilosInRegion
            };
            var siloKeysPerRegion = new Dictionary<string, List<string>>();
            siloKeysPerRegion.Add("EU", new List<string>() {"EU-EU-0", "EU-EU-1"});
            var localConfiguration = new LocalCoordinatorConfiguration()
            {
                SiloIdPerRegion = siloKeysPerRegion
            };

            this.ConfigureRegionalGrains(siloBuilder, regionalSiloPlacementInfo, regionalConfiguration, localConfiguration, localSiloPlacementInfo);
        }

        public void Dispose()
        {

        }

        private void ConfigureRegionalGrains(ISiloBuilder siloHostBuilder,
                                             RegionalSilosPlacementInfo regionalSilos,
                                             RegionalCoordinatorConfiguration regionalConfiguration,
                                             LocalCoordinatorConfiguration localConfiguration,
                                             LocalSiloPlacementInfo localSilos)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddSingleton<ILogger>(this.CreateLogger());

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

        private ILogger CreateLogger()
        {
            /*return new LoggerConfiguration()
                        .WriteTo.File(this.logPath).Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .WriteTo.Console().Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .CreateLogger();
                        */
            return new LoggerConfiguration()
                        .WriteTo.File(this.logPath)
                        .WriteTo.Console()
                        .CreateLogger();
        }
    }
}