using System.Net;
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
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Placement;
using Serilog;
using Serilog.Filters;

namespace GeoSnapperDeployment
{
    public class LocalSiloDeployer
    {
        private readonly ISiloConfigurationFactory siloConfigurationFactory;
        private readonly string logPath = Path.Combine(Utilities.Constants.LogPath, $"Snapper-{DateTime.Now:ddMMyyyy-HHmm}.log");
        private ILogger logger;

        public LocalSiloDeployer(ISiloConfigurationFactory siloConfigurationFactory)
        {
            this.siloConfigurationFactory = siloConfigurationFactory ?? throw new ArgumentNullException(nameof(siloConfigurationFactory));
            this.logger = CreateLogger();
        }

        public async Task<ISiloHost> DeployPrimarySilo(SiloConfigurations siloConfigurations)
        {
            SiloHostBuilder siloHostBuilder = new SiloHostBuilder();
            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;

            IPEndPoint nullPrimarySiloEndpoint = null;

            this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId,
                                                  siloConfigurations.ServiceId, nullPrimarySiloEndpoint,
                                                  primarySiloConfiguration.SiloPort, primarySiloConfiguration.GatewayPort);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            // Configure global silo
            GlobalConfiguration globalConfiguration = this.siloConfigurationFactory.CreateGlobalConfiguration(siloConfigurations.Silos);
            var globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);

            this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, globalSiloInfo, regions);

            // Configure regional silos
            RegionalSilosPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSilos(siloConfigurations);
            RegionalConfiguration regionalConfiguration = this.siloConfigurationFactory.CreateRegionalConfiguration(siloConfigurations.Silos.LocalSilos);
            LocalConfiguration localConfiguration = this.siloConfigurationFactory.CreateLocalConfiguration(siloConfigurations.Silos.LocalSilos);
            var localSiloInfo = this.siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);
            this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSiloInfo, regions);

            // Configure the local silos
            this.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo, regions);

            ISiloHost siloHost = siloHostBuilder.Build();
            await siloHost.StartAsync();

            Console.WriteLine($"Silo primary is started...");
            return siloHost;
        }

        public async Task<ISiloHost> DeployGlobalSilo(SiloConfigurations siloConfigurations)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;
            var siloHostBuilder = new SiloHostBuilder();

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);
            this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                             primarySiloEndpoint, globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort);

            GlobalConfiguration globalConfiguration = this.siloConfigurationFactory.CreateGlobalConfiguration(siloConfigurations.Silos);
            var globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);
            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);



            this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, globalSiloInfo, regions);

            var siloHost = siloHostBuilder.Build();

            await siloHost.StartAsync();

            Console.WriteLine($"Global silo {globalSiloConfiguration.SiloId} in region {globalSiloConfiguration.Region} is started");

            return siloHost;
        }

        public async Task<IList<ISiloHost>> DeployRegionalSilos(SiloConfigurations siloConfigurations)
        {
            Console.WriteLine(string.Join(" ,", siloConfigurations.Silos.RegionalSilos));
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            IReadOnlyList<SiloConfiguration> silos = siloConfigurations.Silos.RegionalSilos;

            RegionalSilosPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSilos(siloConfigurations);
            RegionalConfiguration regionalConfiguration = this.siloConfigurationFactory.CreateRegionalConfiguration(siloConfigurations.Silos.LocalSilos);
            LocalConfiguration localConfiguration = this.siloConfigurationFactory.CreateLocalConfiguration(siloConfigurations.Silos.LocalSilos);
            LocalSiloPlacementInfo localSilos = this.siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);
            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            foreach (SiloConfiguration siloConfiguration in silos)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                                      primarySiloEndpoint, siloConfiguration.SiloPort, siloConfiguration.GatewayPort);

                this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSilos, regions);

                var siloHost = siloHostBuilder.Build();

                await siloHost.StartAsync();

                Console.WriteLine($"Silo regional {siloConfiguration.SiloId} in region {siloConfiguration.Region} is started...");

                siloHosts.Add(siloHost);
            }

            return siloHosts;
        }

        public async Task<IList<ISiloHost>> DeploySilosAndReplicas(SiloConfigurations siloConfigurations)
        {
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            var silos = siloConfigurations.Silos.LocalSilos;

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);
            LocalSiloPlacementInfo localSiloInfo = this.siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);
            RegionalSilosPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSilos(siloConfigurations);
            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            foreach ((string siloRegion, SiloInfo siloInfo) in localSiloInfo.LocalSiloInfo)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo, regions);

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                      siloInfo.ClusterId,
                                                      siloInfo.ServiceId,
                                                      primarySiloEndpoint,
                                                      siloInfo.SiloPort, siloInfo.GatewayPort);

                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} in region {siloRegion} is started...");

                siloHosts.Add(siloHost);
            }

            await Task.WhenAll(startSiloTasks);

            return siloHosts;
        }

        private void ConfigureLocalDeploymentSiloHost(SiloHostBuilder siloHostBuilder,
                                                      string clusterId,
                                                      string serviceId,
                                                      IPEndPoint primarySiloEndpoint,
                                                      int siloPort,
                                                      int gatewayPort)
        {
            siloHostBuilder.UseDevelopmentClustering(primarySiloEndpoint);
            siloHostBuilder.ConfigureEndpoints(IPAddress.Loopback, siloPort, gatewayPort)
                           .UseDashboard(options =>
                           {
                               options.Port = siloPort + 100; // e.g. 11211, TODO: Find something nicer
                               options.Host = "*";
                               options.HostSelf = true;
                               options.CounterUpdateIntervalMs = 10000;
                           })
                           .Configure<EndpointOptions>(options =>
                           {
                               options.AdvertisedIPAddress = IPAddress.Loopback;
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

        private List<string> GetRegions(IReadOnlyList<SiloConfiguration> localSilos)
        {
            return localSilos.Select(localSilo => localSilo.Region)
                             .Distinct()
                             .ToList();
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

                serviceCollection.AddSingleton<IIdHelper, IdHelper>();
                serviceCollection.AddSingleton<ITransactionBroadCasterFactory, TransactionBroadCasterFactory>();
                serviceCollection.AddSingleton<IPlacementManager, PlacementManager>();
                serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
                serviceCollection.AddSingleton<IScheduleInfoManager, ScheduleInfoManager>();
                serviceCollection.AddSingleton<ITransactionSchedulerFactory, TransactionSchedulerFactory>();
                serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();
                serviceCollection.AddSingleton<ILocalDeterministicTransactionProcessorFactory, LocalDeterministicTransactionProcessorFactory>();
                serviceCollection.AddSingleton<IDeterministicTransactionExecutorFactory, DeterministicTransactionExecutorFactory>();

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordinatorGrainPlacementStrategy>(nameof(GlobalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordinatorGrainPlacement>(typeof(GlobalCoordinatorGrainPlacementStrategy));
            });
        }

        private void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder,
                                             RegionalSilosPlacementInfo regionalSilos,
                                             RegionalConfiguration regionalConfiguration,
                                             LocalConfiguration localConfiguration,
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
                serviceCollection.AddSingleton<IPlacementManager, PlacementManager>();
                serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();
                serviceCollection.AddSingleton<ILocalDeterministicTransactionProcessorFactory, LocalDeterministicTransactionProcessorFactory>();
                serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();

                serviceCollection.AddSingleton(regions);
                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(regionalConfiguration);
                serviceCollection.AddSingleton(localConfiguration);
                serviceCollection.AddSingleton(localSilos);

                serviceCollection.AddSingleton<IScheduleInfoManager, ScheduleInfoManager>();
                serviceCollection.AddSingleton<ITransactionSchedulerFactory, TransactionSchedulerFactory>();

                serviceCollection.AddSingleton<IDeterministicTransactionExecutorFactory, DeterministicTransactionExecutorFactory>();

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

        private void ConfigureLocalGrains(SiloHostBuilder siloHostBuilder, RegionalSilosPlacementInfo regionalSilos, LocalSiloPlacementInfo localSilos, List<string> regions)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddLogging(builder =>
                {
                    builder.AddSerilog(CreateLogger());
                });
                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(localSilos);

                serviceCollection.AddSingleton(regions);

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

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
            });
        }

        private ILogger CreateLogger()
        {
            return new LoggerConfiguration()
                        .WriteTo.File(this.logPath).Filter.ByExcluding(Matching.FromSource("Orleans"))
                        // .WriteTo.Console().Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .WriteTo.Console()
                        .CreateLogger();
        }
    }
}