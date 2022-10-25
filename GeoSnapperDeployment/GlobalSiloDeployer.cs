using System.Net;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.TransactionExecution.TransactionContextProvider;
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
    //     private readonly string logPath = Path.Combine(Utilities.Constants.LogPath, $"Snapper-{DateTime.Now:ddMMyyyy-HHmm}.log");
    //     private readonly ISiloConfigurationFactory siloConfigurationFactory;
    //     private readonly ILogger logger;

    //     public GlobalSiloDeployer(ISiloConfigurationFactory siloConfigurationFactory)
    //     {
    //         this.siloConfigurationFactory = siloConfigurationFactory ?? throw new ArgumentNullException(nameof(siloConfigurationFactory));
    //         this.logger = CreateLogger();
    //     }

    //     public async Task Deploy(string region, SiloConfigurations siloConfigurations)
    //     {

    //     }

    //     public async Task<ISiloHost> DeployGlobalSilo(SiloConfigurations siloConfigurations, string region)
    //     {
    //         SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;
    //         IPAddress ipAddress = IPAddress.Parse(globalSiloConfiguration.IPAddress);

    //         var siloHostBuilder = new SiloHostBuilder();

    //         IPEndPoint primarySiloEndpoint = new IPEndPoint(ipAddress, siloConfigurations.Silos.PrimarySilo.SiloPort);

    //         GlobalConfiguration globalConfiguration = this.siloConfigurationFactory.CreateGlobalConfiguration(siloConfigurations.Silos);
    //         var globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);

    //         this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, globalSiloInfo);

    //         var siloHost = siloHostBuilder.Build();

    //         await siloHost.StartAsync();

    //         Console.WriteLine($"Global silo {globalSiloConfiguration.SiloId} in region {globalSiloConfiguration.Region} is started");

    //         return siloHost;
    //     }

    //     public async Task<IList<ISiloHost>> DeployRegionalSilos(SiloConfigurations siloConfigurations, string region)
    //     {
    //         Console.WriteLine(string.Join(" ,", siloConfigurations.Silos.RegionalSilos));
    //         var siloHosts = new List<ISiloHost>();
    //         var startSiloTasks = new List<Task>();
    //         IReadOnlyList<SiloConfiguration> silos = siloConfigurations.Silos.RegionalSilos;

    //         RegionalSilosPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);
    //         RegionalCoordinatorConfiguration regionalConfiguration = this.siloConfigurationFactory.CreateRegionalConfiguration(siloConfigurations.Silos.LocalSilos);
    //         LocalCoordinatorConfiguration localConfiguration = this.siloConfigurationFactory.CreateLocalCoordinatorConfigurationForMaster(siloConfigurations.Silos.LocalSilos);
    //         LocalSiloPlacementInfo localSilos = this.siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);

    //         foreach (SiloConfiguration regionalSiloConfiguration in silos)
    //         {
    //             var siloHostBuilder = new SiloHostBuilder();
    //             IPAddress ipAddress = IPAddress.Parse(regionalSiloConfiguration.IPAddress);
    //             IPEndPoint primarySiloEndpoint = new IPEndPoint(ipAddress, siloConfigurations.Silos.PrimarySilo.SiloPort);

    //             this.ConfigureSiloHost(siloHostBuilder,
    //                                    siloConfigurations.ClusterId,
    //                                    siloConfigurations.ServiceId,
    //                                    primarySiloEndpoint,
    //                                    regionalSiloConfiguration.SiloPort,
    //                                    regionalSiloConfiguration.GatewayPort);

    //             this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSilos);

    //             var siloHost = siloHostBuilder.Build();

    //             await siloHost.StartAsync();

    //             Console.WriteLine($"Silo regional {regionalSiloConfiguration.SiloId} in region {regionalSiloConfiguration.Region} is started...");

    //             siloHosts.Add(siloHost);
    //         }

    //         return siloHosts;
    //     }

    //     public async Task<IList<ISiloHost>> DeployLocalSilosAndReplicas(SiloConfigurations siloConfigurations, string region)
    //     {
    //         var siloHosts = new List<ISiloHost>();
    //         var startSiloTasks = new List<Task>();

    //         IReadOnlyCollection<SiloConfiguration> silos = siloConfigurations.Silos.LocalSilos;
    //         IEnumerable<SiloConfiguration> homeSilos = silos.Where(config => config.Region.Equals(region));
    //         IEnumerable<SiloConfiguration> replicaSilos = silos.Where(config => !config.Region.Equals(region));

    //         RegionalSilosPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);

    //         foreach(SiloConfiguration localSiloConfiguration in homeSilos)
    //         {
    //             this.ConfigureSiloHost(siloHostBuilder,
    //                                    siloInfo.ClusterId,
    //                                    siloInfo.ServiceId,
    //                                    primarySiloEndpoint,
    //                                    siloInfo.SiloPort,
    //                                    siloInfo.GatewayPort);
    //         }

    //         foreach(SiloConfiguration localSiloConfiguration in replicaSilos)
    //         {

    //         }

    //         foreach ((string siloRegion, SiloInfo siloInfo) in localSiloInfo.LocalSiloInfo)
    //         {
    //             IPAddress ipAddress = IPAddress.Parse(regionalSiloConfiguration.IPAddress);
    //             IPEndPoint primarySiloEndpoint = new IPEndPoint(ipAddress, siloConfigurations.Silos.PrimarySilo.SiloPort);
    //             var siloHostBuilder = new SiloHostBuilder();

    //             this.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo);

    //             this.ConfigureSiloHost(siloHostBuilder,
    //                                    siloInfo.ClusterId,
    //                                    siloInfo.ServiceId,
    //                                    primarySiloEndpoint,
    //                                    siloInfo.SiloPort,
    //                                    siloInfo.GatewayPort);

    //             var siloHost = siloHostBuilder.Build();

    //             startSiloTasks.Add(siloHost.StartAsync());

    //             Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} in region {siloRegion} is started...");

    //             siloHosts.Add(siloHost);
    //         }

    //         await Task.WhenAll(startSiloTasks);

    //         return siloHosts;
    //     }

    //     private void ConfigureSiloHost(SiloHostBuilder siloHostBuilder,
    //                                    string clusterId,
    //                                    string serviceId,
    //                                    IPEndPoint primarySiloEndpoint,
    //                                    int siloPort,
    //                                    int gatewayPort)
    //     {
    //         siloHostBuilder.ConfigureEndpoints(IPAddress.Loopback, siloPort, gatewayPort)
    //                        .UseDashboard(options =>
    //                        {
    //                            options.Port = siloPort + 100; // e.g. 11211, TODO: Find something nicer
    //                            options.Host = "*";
    //                            options.HostSelf = true;
    //                            options.CounterUpdateIntervalMs = 10000;
    //                        })
    //                        .Configure<EndpointOptions>(options =>
    //                        {
    //                            options.AdvertisedIPAddress = IPAddress.Loopback;
    //                        })
    //                        .Configure<ClientMessagingOptions>(options =>
    //                        {
    //                            options.ResponseTimeout = new TimeSpan(0, 5, 0);
    //                            options.ResponseTimeoutWithDebugger = new TimeSpan(0, 5, 0);
    //                        })
    //                        .Configure<ClusterOptions>(options =>
    //                        {
    //                            options.ClusterId = clusterId;
    //                            options.ServiceId = serviceId;
    //                        });
    //         //.ConfigureLogging(logging => logging.AddConsole());
    //     }

    //     private void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration, SiloInfo globalSiloInfo)
    //     {
    //         siloHostBuilder.ConfigureServices(serviceCollection =>
    //         {
    //             serviceCollection.AddLogging(builder =>
    //             {
    //                 builder.AddSerilog(this.logger);
    //             });

    //             serviceCollection.AddSingleton(globalConfiguration);
    //             serviceCollection.AddSingleton(globalSiloInfo);

    //             serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
    //             serviceCollection.AddSingleton<IPlacementManager, PlacementManager>();
    //             serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordinatorGrainPlacementStrategy>(nameof(GlobalCoordinatorGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordinatorGrainPlacement>(typeof(GlobalCoordinatorGrainPlacementStrategy));
    //         });
    //     }

    //     private void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder,
    //                                          RegionalSilosPlacementInfo regionalSilos,
    //                                          RegionalCoordinatorConfiguration regionalConfiguration,
    //                                          LocalCoordinatorConfiguration localConfiguration,
    //                                          LocalSiloPlacementInfo localSilos)
    //     {
    //         siloHostBuilder.ConfigureServices(serviceCollection =>
    //         {
    //             serviceCollection.AddLogging(builder =>
    //             {
    //                 builder.AddSerilog(this.logger);
    //             });

    //             serviceCollection.AddSingleton(regionalSilos);
    //             serviceCollection.AddSingleton(regionalConfiguration);
    //             serviceCollection.AddSingleton(localConfiguration);
    //             serviceCollection.AddSingleton(localSilos);

    //             serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
    //             serviceCollection.AddSingleton<IPlacementManager, PlacementManager>();
    //             serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();
    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalConfigurationGrainPlacementStrategy>(nameof(RegionalConfigurationGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigurationGrainPlacement>(typeof(RegionalConfigurationGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
    //         });
    //     }

    //     private void ConfigureLocalGrains(SiloHostBuilder siloHostBuilder, RegionalSilosPlacementInfo regionalSilos, LocalSiloPlacementInfo localSilos)
    //     {
    //         siloHostBuilder.ConfigureServices(serviceCollection =>
    //         {
    //             serviceCollection.AddLogging(builder =>
    //             {
    //                 builder.AddSerilog(this.logger);
    //             });
    //             serviceCollection.AddSingleton(regionalSilos);
    //             serviceCollection.AddSingleton(localSilos);
    //             // serviceCollection.AddSingleton(logger);

    //             serviceCollection.AddSingleton<ITransactionContextProviderFactory, TransactionContextProviderFactory>();
    //             serviceCollection.AddSingleton<IPlacementManager, PlacementManager>();
    //             serviceCollection.AddSingleton<ICoordinatorProvider, CoordinatorProvider>();
    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalConfigurationGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigurationGrainPlacement>(typeof(LocalConfigurationGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigurationGrainPlacementStrategy>(nameof(LocalCoordinatorGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordinatorGrainPlacement>(typeof(LocalCoordinatorGrainPlacementStrategy));

    //             serviceCollection.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
    //             serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
    //         });
    //     }

    //     private ILogger CreateLogger()
    //     {
    //         return new LoggerConfiguration()
    //                     .WriteTo.File(this.logPath).Filter.ByExcluding(Matching.FromSource("Orleans"))
    //                     .WriteTo.Console().Filter.ByExcluding(Matching.FromSource("Orleans"))
    //                     .CreateLogger();
    //     }
    }
}