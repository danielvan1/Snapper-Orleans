using System.Net;
using Concurrency.Implementation;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.TransactionBroadcasting;
using Concurrency.Implementation.TransactionExecution.Scheduler;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
using Concurrency.Implementation.TransactionExecution.TransactionExecution;
using Concurrency.Implementation.TransactionExecution.TransactionPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
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
    public static class SiloConfigurationManager
    {
        private static readonly string logPath = Path.Combine(Utilities.Constants.LogPath, $"Snapper-{DateTime.Now:ddMMyyyy-HHmm}.log");

        public static void ConfigureGlobalDeploymentSiloHost(SiloHostBuilder siloHostBuilder,
                                                             IPAddress advertisedSiloIPAdress,
                                                             string clusterId,
                                                             string serviceId,
                                                             int siloPort,
                                                             int gatewayPort)
        {
            const string key1 = "";
            Console.WriteLine($"IP: {advertisedSiloIPAdress}, siloPort: {siloPort}, gatewayPort: {gatewayPort}, localhost: {IPAddress.Loopback}");

            siloHostBuilder.Configure<EndpointOptions>(options =>
                            {
                                options.AdvertisedIPAddress = advertisedSiloIPAdress;
                                options.SiloListeningEndpoint = new IPEndPoint(IPAddress.Parse("0.0.0.0"), siloPort);
                                options.GatewayListeningEndpoint = new IPEndPoint(IPAddress.Parse("0.0.0.0"), gatewayPort);
                                options.SiloPort = siloPort;
                                options.GatewayPort = gatewayPort;
                            })
                           .UseDashboard(options =>
                           {
                               options.Port = siloPort + 100; // e.g. 11211, TODO: Find something nicer
                               options.Host = "*";
                               options.HostSelf = true;
                               options.CounterUpdateIntervalMs = 10000;
                           })
                           .Configure<ClientMessagingOptions>(options =>
                           {
                               options.ResponseTimeout = new TimeSpan(0, 10, 0);
                               options.ResponseTimeoutWithDebugger = new TimeSpan(0, 10, 0);
                               options.BufferPoolMaxSize = 100000;
                               options.BufferPoolBufferSize = 100000;
                               options.DropExpiredMessages = false;
                           })
                           .Configure<ClusterMembershipOptions>(options =>
                           {
                               options.DeathVoteExpirationTimeout = new TimeSpan(0, 10, 0);
                               options.IAmAliveTablePublishTimeout = new TimeSpan(0, 10, 0);
                               options.NumMissedTableIAmAliveLimit = 1000000;
                               options.NumVotesForDeathDeclaration = 1000000;
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
                           .UseAzureStorageClustering(options => options.ConfigureTableServiceClient(key1));
        }

        public static void ConfigureLocalDeploymentSiloHost(SiloHostBuilder siloHostBuilder,
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
                               options.ResponseTimeout = new TimeSpan(0, 10, 0);
                               options.ResponseTimeoutWithDebugger = new TimeSpan(0, 10, 0);
                               options.BufferPoolMaxSize = 100000;
                               options.BufferPoolBufferSize = 100000;
                               options.DropExpiredMessages = false;
                           })
                           .Configure<ClusterMembershipOptions>(options =>
                           {
                               options.DeathVoteExpirationTimeout = new TimeSpan(0, 10, 0);
                               options.IAmAliveTablePublishTimeout = new TimeSpan(0, 10, 0);
                               options.NumMissedTableIAmAliveLimit = 1000000;
                               options.NumVotesForDeathDeclaration = 1000000;
                           })
                           .Configure<SiloMessagingOptions>(options => {
                                options.ResponseTimeout = new TimeSpan(0, 5, 0);
                                options.ResponseTimeoutWithDebugger = new TimeSpan(0, 5, 0);
                            })
                           .Configure<ClusterOptions>(options =>
                           {
                               options.ClusterId = clusterId;
                               options.ServiceId = serviceId;
                           });
        }


        public static void ConfigurePrimarySilo(SiloHostBuilder siloHostBuilder, SiloInfo globalSiloInfo, RegionalSiloPlacementInfo regionalSiloPlacementInfo, LocalSiloPlacementInfo localSiloPlacementInfo)
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

        public static void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration, SiloInfo globalSiloInfo, List<string> regions)
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

        public static void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder,
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

        public static void ConfigureLocalGrains(SiloHostBuilder siloHostBuilder,
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

        public static void ConfigureGlobalDeploymentPrimarySilo(SiloHostBuilder siloHostBuilder,
                                                                RegionalSiloPlacementInfo regionalSiloPlacementInfo,
                                                                LocalSiloPlacementInfo localSiloPlacementInfo)
        {
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddLogging(builder =>
                {
                    builder.AddSerilog(CreateLogger());
                });

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

        public static void ConfigureGlobalDeploymentGlobalGrains(SiloHostBuilder siloHostBuilder,
                                                                 GlobalConfiguration globalConfiguration,
                                                                 SiloInfo globalSiloInfo,
                                                                 List<string> regions)
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

        public static void ConfigureGlobalDeploymentRegionalGrains(SiloHostBuilder siloHostBuilder,
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
            });
        }

        public static void ConfigureGlobalDeploymentLocalGrains(SiloHostBuilder siloHostBuilder,
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
                serviceCollection.AddSingleton<IScheduleInfoManager, ScheduleInfoManager>();
                serviceCollection.AddSingleton<ITransactionSchedulerFactory, TransactionSchedulerFactory>();

                serviceCollection.AddSingleton<IScheduleInfoManagerFactory, ScheduleInfoManagerFactory >();
                serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
                serviceCollection.AddSingleton<ITransactionBroadCasterFactory, TransactionBroadCasterFactory>();
                serviceCollection.AddSingleton<IDeterministicTransactionExecutorFactory, DeterministicTransactionExecutorFactory>();
                serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();
            });
        }

        private static ILogger CreateLogger()
        {
            return new LoggerConfiguration()
                        .WriteTo.File(logPath).Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .WriteTo.Console().Filter.ByExcluding(Matching.FromSource("Orleans"))
                        .MinimumLevel.Fatal()
                        .CreateLogger();
        }
    }
}