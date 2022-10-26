using System.Net;
using System.Reflection.Metadata.Ecma335;
using Concurrency.Implementation;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.Coordinator.Local;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.TransactionBroadcasting;
using Concurrency.Implementation.TransactionExecution.Scheduler;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Implementation.TransactionExecution.TransactionExecution;
using Concurrency.Implementation.TransactionExecution.TransactionPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Placement;
using Serilog;
using Serilog.Filters;

namespace GeoSnapperDeployment
{
    public class GlobalSiloDeployer
    {
        private readonly string logPath = Path.Combine(Utilities.Constants.LogPath, $"Snapper-{DateTime.Now:ddMMyyyy-HHmm}.log");
        private readonly ISiloConfigurationFactory siloConfigurationFactory;
        private readonly ILogger logger;

        public GlobalSiloDeployer(ISiloConfigurationFactory siloConfigurationFactory)
        {
            this.siloConfigurationFactory = siloConfigurationFactory ?? throw new ArgumentNullException(nameof(siloConfigurationFactory));
            this.logger = CreateLogger();
        }

        public async Task<List<ISiloHost>> Deploy(SiloConfigurations siloConfigurations, string region)
        {
            var siloHostTasks = new List<Task<ISiloHost>>();

            if(siloConfigurations.Silos.PrimarySilo.Region.Equals(region))
            {
                siloHostTasks.Add(this.DeployPrimarySilo(siloConfigurations));
            }

            if(siloConfigurations.Silos.GlobalSilo.Region.Equals(region))
            {
                siloHostTasks.Add(this.DeployGlobalSilo(siloConfigurations, region));
            }

            var global = await Task.WhenAll(siloHostTasks);

            var regional = await this.DeployRegionalSilos(siloConfigurations, region);
            var localTasks = await this.DeployLocalSilosAndReplicas(siloConfigurations, region);

            return global.Concat(regional)
                         .Concat(localTasks)
                         .ToList();
        }

        public async Task<ISiloHost> DeployPrimarySilo(SiloConfigurations siloConfigurations)
        {
            SiloHostBuilder siloHostBuilder = new SiloHostBuilder();

            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;
            IPAddress IPAddress = IPAddress.Parse(primarySiloConfiguration.IPAddress);
            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress, siloConfigurations.Silos.PrimarySilo.SiloPort);

            this.ConfigureSiloHost(siloHostBuilder,
                                   null,
                                   IPAddress,
                                   siloConfigurations.ClusterId,
                                   siloConfigurations.ServiceId,
                                   primarySiloConfiguration.SiloPort,
                                   primarySiloConfiguration.GatewayPort);


            SiloInfo globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);
            RegionalSiloPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);
            LocalSiloPlacementInfo localSilos = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations);

            this.ConfigurePrimarySilo(siloHostBuilder, globalSiloInfo, regionalSilos, localSilos);

            ISiloHost siloHost = siloHostBuilder.Build();
            await siloHost.StartAsync();

            Console.WriteLine($"Silo primary is started...");

            return siloHost;
        }


        public async Task<ISiloHost> DeployGlobalSilo(SiloConfigurations siloConfigurations, string region)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;
            var siloHostBuilder = new SiloHostBuilder();

            GlobalConfiguration globalConfiguration = this.siloConfigurationFactory.CreateGlobalConfiguration(siloConfigurations.Silos);
            var globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);

            // this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, globalSiloInfo);

            var siloHost = siloHostBuilder.Build();

            await siloHost.StartAsync();

            Console.WriteLine($"Global silo {globalSiloConfiguration.SiloId} in region {globalSiloConfiguration.Region} is started");

            return siloHost;
        }

        public async Task<IList<ISiloHost>> DeployRegionalSilos(SiloConfigurations siloConfigurations, string region)
        {
            Console.WriteLine(string.Join(" ,", siloConfigurations.Silos.RegionalSilos));
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();

            IEnumerable<SiloConfiguration> silosToDeploy = siloConfigurations.Silos.RegionalSilos.Where(config => config.Region.Equals(region));

            RegionalSiloPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);
            RegionalCoordinatorConfiguration regionalConfiguration = this.siloConfigurationFactory.CreateRegionalConfiguration(siloConfigurations.Silos.LocalSilos);
            LocalCoordinatorConfiguration localConfiguration = this.siloConfigurationFactory.CreateLocalCoordinatorConfigurationWithReplica(siloConfigurations.Silos.LocalSilos);
            LocalSiloPlacementInfo localSilos = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations);

            string IPAddressString = siloConfigurations.IPAddresses.Where(ip => ip.Region.Equals(region)).First().IPAddress;

            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;
            IPAddress IPAddress = IPAddress.Parse(primarySiloConfiguration.IPAddress);
            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress, siloConfigurations.Silos.PrimarySilo.SiloPort);
            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            foreach (SiloConfiguration regionalSiloConfiguration in silosToDeploy)
            {
                var siloHostBuilder = new SiloHostBuilder();
                IPAddress siloIPAddress = IPAddress.Parse(regionalSiloConfiguration.IPAddress);

                this.ConfigureSiloHost(siloHostBuilder,
                                       primarySiloEndpoint,
                                       siloIPAddress,
                                       siloConfigurations.ClusterId,
                                       siloConfigurations.ServiceId,
                                       regionalSiloConfiguration.SiloPort,
                                       regionalSiloConfiguration.GatewayPort);

                this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSilos, regions);

                var siloHost = siloHostBuilder.Build();

                await siloHost.StartAsync();

                Console.WriteLine($"Silo regional {regionalSiloConfiguration.SiloId} in region {regionalSiloConfiguration.Region} is started...");

                siloHosts.Add(siloHost);
            }

            return siloHosts;
        }

        public async Task<IList<ISiloHost>> DeployLocalSilosAndReplicas(SiloConfigurations siloConfigurations, string region)
        {
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();

            IReadOnlyCollection<SiloConfiguration> silos = siloConfigurations.Silos.LocalSilos;

            LocalSiloPlacementInfo localSiloInfo = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations);
            RegionalSiloPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);

            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;
            IPAddress IPAddress = IPAddress.Parse(primarySiloConfiguration.IPAddress);
            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress, siloConfigurations.Silos.PrimarySilo.SiloPort);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            foreach ((string siloRegion, SiloInfo siloInfo) in localSiloInfo.LocalSiloInfo.Where(kv => kv.Key.Equals(region)))
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo, regions);

                this.ConfigureSiloHost(siloHostBuilder,
                                       primarySiloEndpoint,
                                       siloInfo.IPEndPoint.Address,
                                       siloInfo.ClusterId,
                                       siloInfo.ServiceId,
                                       siloInfo.SiloPort, siloInfo.GatewayPort);

                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} in region {siloRegion} is started...");

                siloHosts.Add(siloHost);
            }

            await Task.WhenAll(startSiloTasks);

            return siloHosts;
        }

        private void ConfigureSiloHost(SiloHostBuilder siloHostBuilder,
                                       IPEndPoint primarySiloEndPoint,
                                       IPAddress siloIPAdress,
                                       string clusterId,
                                       string serviceId,
                                       int siloPort,
                                       int gatewayPort)
        {
            siloHostBuilder.UseDevelopmentClustering(primarySiloEndPoint);
            siloHostBuilder.ConfigureEndpoints(siloIPAdress, siloPort, gatewayPort)
                           .UseDashboard(options =>
                           {
                               options.Port = siloPort + 100; // e.g. 11211, TODO: Find something nicer
                               options.Host = "*";
                               options.HostSelf = true;
                               options.CounterUpdateIntervalMs = 10000;
                           })
                           .Configure<ClientMessagingOptions>(options =>
                           {
                               options.ResponseTimeout = new TimeSpan(0, 5, 0);
                               options.ResponseTimeoutWithDebugger = new TimeSpan(0, 5, 0);
                           })
                           .Configure<ClusterOptions>(options =>
                           {
                               options.ClusterId = clusterId;
                               options.ServiceId = serviceId;
                           });
        }

        private ILogger CreateLogger()
        {
            return new LoggerConfiguration()
                        .WriteTo.File(this.logPath).Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .WriteTo.Console().Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .CreateLogger();
        }

        private List<string> GetRegions(IReadOnlyList<SiloConfiguration> localSilos)
        {
            return localSilos.Select(localSilo => localSilo.Region)
                             .Distinct()
                             .ToList();
        }

        private void ConfigurePrimarySilo(SiloHostBuilder siloHostBuilder, SiloInfo globalSiloInfo, RegionalSiloPlacementInfo regionalSiloPlacementInfo, LocalSiloPlacementInfo localSiloPlacementInfo)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddLogging(builder =>
                {
                    builder.AddSerilog(CreateLogger());
                });

                serviceCollection.AddSingleton(globalSiloInfo);
                serviceCollection.AddSingleton(regionalSiloPlacementInfo);
                serviceCollection.AddSingleton(localSiloPlacementInfo);

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordinatorGrainPlacementStrategy>(nameof(GlobalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordinatorGrainPlacement>(typeof(GlobalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalConfigurationGrainPlacementStrategy>(nameof(RegionalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigurationGrainPlacement>(typeof(RegionalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalCoordinatorGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));

            });

        }

        private void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration, SiloInfo globalSiloInfo, List<string> regions)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddLogging(builder =>
                {
                    builder.AddSerilog(CreateLogger());
                });

                serviceCollection.AddSingleton(globalConfiguration);
                serviceCollection.AddSingleton(globalSiloInfo);
                serviceCollection.AddSingleton(regions);
            });
        }

        private void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder,
                                             RegionalSiloPlacementInfo regionalSilos,
                                             RegionalCoordinatorConfiguration regionalConfiguration,
                                             LocalCoordinatorConfiguration localConfiguration,
                                             LocalSiloPlacementInfo localSilos,
                                             List<string> regions)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddLogging(builder =>
                {
                    builder.AddSerilog(CreateLogger());
                });

                serviceCollection.AddSingleton<IIdHelper, IdHelper>();
                serviceCollection.AddSingleton<ITransactionBroadCasterFactory, TransactionBroadCasterFactory>();
                serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();

                serviceCollection.AddSingleton(regions);
                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(regionalConfiguration);
                serviceCollection.AddSingleton(localConfiguration);
                serviceCollection.AddSingleton(localSilos);



                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalCoordinatorGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalConfigurationGrainPlacementStrategy>(nameof(RegionalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigurationGrainPlacement>(typeof(RegionalConfigurationGrainPlacementStrategy));
            });
        }

        private void ConfigureLocalGrains(SiloHostBuilder siloHostBuilder, RegionalSiloPlacementInfo regionalSilos, LocalSiloPlacementInfo localSilos, List<string> regions)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddLogging(builder =>
                {
                    builder.AddSerilog(CreateLogger());
                });

                serviceCollection.AddSingleton(regions);
                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(localSilos);

                serviceCollection.AddSingleton<IIdHelper, IdHelper>();
                serviceCollection.AddSingleton<IPlacementManager, PlacementManager>();
                serviceCollection.AddSingleton<IScheduleInfoManager, ScheduleInfoManager>();
                serviceCollection.AddSingleton<ITransactionSchedulerFactory, TransactionSchedulerFactory>();

                serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
                serviceCollection.AddSingleton<ITransactionBroadCasterFactory, TransactionBroadCasterFactory>();
                serviceCollection.AddSingleton<IDeterministicTransactionExecutorFactory, DeterministicTransactionExecutorFactory>();
                serviceCollection.AddSingleton<ILocalDeterministicTransactionProcessorFactory, LocalDeterministicTransactionProcessorFactory>();
                serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalCoordinatorGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
            });
        }

    }
}