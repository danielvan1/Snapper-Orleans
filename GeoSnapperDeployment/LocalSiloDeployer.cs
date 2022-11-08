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
using Microsoft.Extensions.Logging;

namespace GeoSnapperDeployment
{
    public class LocalSiloDeployer
    {
        private readonly ISiloConfigurationFactory siloConfigurationFactory;
        private readonly string logPath = Path.Combine(Utilities.Constants.LogPath, $"Snapper-{DateTime.Now:ddMMyyyy-HHmm}.log");
        private Serilog.ILogger logger;

        public LocalSiloDeployer(ISiloConfigurationFactory siloConfigurationFactory)
        {
            this.siloConfigurationFactory = siloConfigurationFactory ?? throw new ArgumentNullException(nameof(siloConfigurationFactory));
            this.logger = CreateLogger();
        }

        public async Task<ISiloHost> DeployPrimarySilo(SiloConfigurations siloConfigurations)
        {
            SiloHostBuilder siloHostBuilder = new SiloHostBuilder();
            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;
            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            this.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                  siloConfigurations.ClusterId,
                                                  siloConfigurations.ServiceId,
                                                  primarySiloEndpoint,
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

        public async Task<ISiloHost> DeployGlobalSilo(SiloConfigurations siloConfigurations)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;
            var siloHostBuilder = new SiloHostBuilder();

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            this.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                  siloConfigurations.ClusterId,
                                                  siloConfigurations.ServiceId,
                                                  primarySiloEndpoint,
                                                  globalSiloConfiguration.SiloPort,
                                                  globalSiloConfiguration.GatewayPort);

            GlobalConfiguration globalConfiguration = this.siloConfigurationFactory.CreateGlobalConfiguration(siloConfigurations.Silos);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            SiloInfo globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);
            this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, globalSiloInfo, regions);

            var siloHost = siloHostBuilder.Build();

            await siloHost.StartAsync();

            Console.WriteLine($"Global silo {globalSiloConfiguration.SiloIntegerId} in region {globalSiloConfiguration.Region} is started");

            return siloHost;
        }

        public async Task<IList<ISiloHost>> DeployRegionalSilos(SiloConfigurations siloConfigurations)
        {
            Console.WriteLine(string.Join(" ,", siloConfigurations.Silos.RegionalSilos));
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            IReadOnlyList<SiloConfiguration> silos = siloConfigurations.Silos.RegionalSilos;

            RegionalSiloPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);
            RegionalCoordinatorConfiguration regionalConfiguration = this.siloConfigurationFactory.CreateRegionalConfiguration(siloConfigurations.Silos.LocalSilos);
            LocalCoordinatorConfiguration localConfiguration = this.siloConfigurationFactory.CreateLocalCoordinatorConfigurationWithReplica(siloConfigurations.Silos.LocalSilos);

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            LocalSiloPlacementInfo localSilos = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations);
            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            foreach (SiloConfiguration siloConfiguration in silos)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                                      primarySiloEndpoint, siloConfiguration.SiloPort, siloConfiguration.GatewayPort);

                this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSilos, regions);

                var siloHost = siloHostBuilder.Build();

                await siloHost.StartAsync();

                Console.WriteLine($"Silo regional {siloConfiguration.SiloIntegerId} in region {siloConfiguration.Region} is started...");

                siloHosts.Add(siloHost);
            }

            return siloHosts;
        }

        public async Task<IList<ISiloHost>> DeployLocalSilosAndReplicas(SiloConfigurations siloConfigurations)
        {
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            var silos = siloConfigurations.Silos.LocalSilos;

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);
            LocalSiloPlacementInfo localSiloInfo = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);
            RegionalSiloPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);

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
                           /*.UseDashboard(options =>
                           {
                               options.Port = siloPort + 100; // e.g. 11211, TODO: Find something nicer
                               options.Host = "*";
                               options.HostSelf = true;
                               options.CounterUpdateIntervalMs = 10000;
                           })*/
                           .Configure<EndpointOptions>(options =>
                           {
                               options.AdvertisedIPAddress = IPAddress.Loopback;
                           })
                           .Configure<ClusterMembershipOptions>(options => {
                                options.DeathVoteExpirationTimeout = new TimeSpan(0, 10, 0);
                                options.IAmAliveTablePublishTimeout = new TimeSpan(0, 10, 0);
                                options.NumMissedTableIAmAliveLimit = 10;
                                options.UseLivenessGossip = true;
                                options.NumMissedProbesLimit = 200;
                                options.EnableIndirectProbes = true;
                                options.ProbeTimeout = new TimeSpan(0, 0, 10);
                           })
                           .Configure<ClientMessagingOptions>(options =>
                           {
                               //options.MaxMessageBodySize = 9999999;
                               //options.MaxMessageHeaderSize = 9999999;
                               //options.DropExpiredMessages = false;
                               options.BufferPoolMaxSize = 10000;
                               options.BufferPoolBufferSize = 10000;
                               options.ResponseTimeout = new TimeSpan(0, 5, 0);
                               options.ResponseTimeoutWithDebugger = new TimeSpan(0, 5, 0);
                           })
                           .Configure<SiloMessagingOptions>(options => {
                                options.ResponseTimeout = new TimeSpan(0, 5, 0);
                                options.ResponseTimeoutWithDebugger = new TimeSpan(0, 5, 0);
                            })
                           .Configure<ClusterOptions>(options =>
                           {
                               options.ClusterId = clusterId;
                               options.ServiceId = serviceId;
                           })
                           .ConfigureLogging(logging => logging.AddConsole());;
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
                serviceCollection.AddSingleton<IScheduleInfoManagerFactory, ScheduleInfoManagerFactory >();

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

        private void ConfigureLocalGrains(SiloHostBuilder siloHostBuilder,
                                          RegionalSiloPlacementInfo regionalSilos,
                                          LocalSiloPlacementInfo localSilos,
                                          List<string> regions)
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
                serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();
                serviceCollection.AddSingleton<ITransactionSchedulerFactory, TransactionSchedulerFactory>();
                serviceCollection.AddSingleton<IScheduleInfoManagerFactory, ScheduleInfoManagerFactory >();
                serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
                serviceCollection.AddSingleton<ITransactionBroadCasterFactory, TransactionBroadCasterFactory>();
                serviceCollection.AddSingleton<IDeterministicTransactionExecutorFactory, DeterministicTransactionExecutorFactory>();

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalCoordinatorGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacementStrategy));
            });
        }

        private Serilog.ILogger CreateLogger()
        {
            return new LoggerConfiguration()
                        .WriteTo.File(this.logPath).Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .WriteTo.Console().Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .MinimumLevel.Fatal()
                        .CreateLogger();
        }
    }
}