using System.Net;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Placement;
using Microsoft.Extensions.Logging;

namespace GeoSnapperDeployment
{
    public class LocalSiloDeployer
    {
        private readonly ISiloInfoFactory siloInfoFactory;

        public LocalSiloDeployer(ISiloInfoFactory siloInfoFactory)
        {
            this.siloInfoFactory = siloInfoFactory ?? throw new ArgumentNullException(nameof(siloInfoFactory));
        }

        public async Task<IList<ISiloHost>> DeploySilosAndReplicas(SiloConfigurations siloConfigurations)
        {
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            var silos = siloConfigurations.Silos.LocalSilos;

            Dictionary<string, SiloInfo> replicas = CreateReplicasDictionary(silos, siloConfigurations.PrimarySiloEndpoint, siloConfigurations.StartGatewayPort);

            foreach((string siloRegion, SiloInfo siloInfo) in replicas)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, 
                                                      siloInfo.ClusterId, 
                                                      siloInfo.ServiceId, 
                                                      siloConfigurations.PrimarySiloEndpoint, 
                                                      siloInfo.SiloPort, siloInfo.GatewayPort);
                
                siloHostBuilder.ConfigureServices(serviceCollection => serviceCollection.AddSingleton(replicas));

                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} in region {siloRegion} is started...");

                siloHosts.Add(siloHost);
            }

            await Task.WhenAll(startSiloTasks);

            return siloHosts;
        }

        public async Task<IList<ISiloHost>> DeployRegionalSilos(SiloConfigurations siloConfigurations)
        {
            Console.WriteLine(string.Join(" ,", siloConfigurations.Silos.RegionalSilos));
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            IReadOnlyList<SiloConfiguration> silos = siloConfigurations.Silos.RegionalSilos;

            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach(SiloConfiguration siloConfiguration in silos)
            {
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfiguration.SiloId,
                                                                siloConfiguration.SiloPort, siloConfiguration.SiloPort, siloConfiguration.Region, 
                                                                siloConfiguration.Region, isReplica);

                regionalSilos.Add(siloConfiguration.Region, siloInfo);
            }

            RegionalConfiguration regionalConfiguration = new RegionalConfiguration()
            {
            };

            foreach(SiloConfiguration siloConfiguration in silos)
            {
                var siloHostBuilder = new SiloHostBuilder();

                ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                                 siloConfigurations.PrimarySiloEndpoint, siloConfiguration.SiloPort, siloConfiguration.GatewayPort);

                ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration);

                var siloHost = siloHostBuilder.Build();

                await siloHost.StartAsync();

                Console.WriteLine($"Silo regional {siloConfiguration.SiloId} in region {siloConfiguration.Region} is started...");

                siloHosts.Add(siloHost);
            }

            return siloHosts;
        }

        public async Task<ISiloHost> DeployGlobalSilo(SiloConfigurations siloConfigurations)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;
            var siloHostBuilder = new SiloHostBuilder();

            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach(SiloConfiguration siloConfiguration in siloConfigurations.Silos.RegionalSilos)
            {
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfiguration.SiloId,
                                                                siloConfiguration.SiloPort, siloConfiguration.SiloPort, siloConfiguration.Region, 
                                                                siloConfiguration.Region, isReplica);

                regionalSilos.Add(siloConfiguration.Region, siloInfo);
            }
            ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                             siloConfigurations.PrimarySiloEndpoint, globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort);
                                            

            var regions = siloConfigurations.Silos.RegionalSilos.Select(regionalSilo => regionalSilo.Region)
                                                                .Distinct()
                                                                .ToList();


            GlobalConfiguration globalConfiguration = new GlobalConfiguration() 
            {
                Regions = regions
            };

            ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, regionalSilos);

            var siloHost = siloHostBuilder.Build();

            await siloHost.StartAsync();

            Console.WriteLine($"Global silo {globalSiloConfiguration.SiloId} in region {globalSiloConfiguration.Region} is started");

            return siloHost;
        }

        private void ConfigureLocalDeploymentSiloHost(SiloHostBuilder siloHostBuilder, 
                                                      string clusterId,
                                                      string serviceId,
                                                      int localPrimarySiloEndpoint,
                                                      int siloPort,
                                                      int gatewayPort)
        {
            if(gatewayPort == localPrimarySiloEndpoint) {
                IPEndPoint nullPrimarySiloEndpoint = null;
                siloHostBuilder.UseDevelopmentClustering(nullPrimarySiloEndpoint);
            } else {
                IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, 11111);
                siloHostBuilder
                .UseDevelopmentClustering(primarySiloEndpoint);
            }
            siloHostBuilder
            .ConfigureEndpoints(IPAddress.Loopback, siloPort, gatewayPort)
            //Can see the dashboard at: http://localhost:8080/#/
            //TODO: Maybe revert it s.t. each dashboard is hosted on its own port
            // Someone suggested it + maybe we can get some extra info for that particular silo
            .UseDashboard(options => { 
                options.Port = 8080;
                options.Host = "*";
                options.HostSelf = true;
                options.CounterUpdateIntervalMs = 1000;
                })
            .Configure<EndpointOptions>(options =>
            {
                options.AdvertisedIPAddress = IPAddress.Loopback;
            })
            .Configure<ClusterOptions>(options =>
            {
                options.ClusterId = clusterId;
                options.ServiceId = serviceId;
            })
            .ConfigureLogging(logging => logging.AddConsole());
        }

        private Dictionary<string, SiloInfo> CreateReplicasDictionary(IReadOnlyList<SiloConfiguration> silos, int startPort, int startGatewayPort)
        {
            var replicas = new Dictionary<string, SiloInfo>();

            // Create main regions
            for(int i = 0; i < silos.Count; i++)
            {
                var siloConfiguration = silos[i];
                int siloId = siloConfiguration.SiloId;
                int siloPort = siloConfiguration.SiloPort;
                int gatewayPort = siloConfiguration.GatewayPort;
                string region = siloConfiguration.Region;
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, region, region, siloId, siloPort, gatewayPort, region, region, isReplica);

                string stringKey = $"{siloInfo.Region}-{siloInfo.Region}";

                replicas.Add(stringKey, siloInfo);
            }

            startPort += silos.Count;
            startGatewayPort += silos.Count;
            int startId = silos.Count;

            // Create replicas
            for(int i = 0; i < silos.Count; i++)
            {
                var currentSiloConfiguration = silos[i];

                for(int j = 0; j < silos.Count; j++)
                {
                    if(i == j) continue;

                    var replicaSiloConfiguration = silos[j];

                    int siloId = startId; 
                    int siloPort = startPort;
                    int gatewayPort = startGatewayPort;
                    string deployRegion = replicaSiloConfiguration.Region;
                    string homeRegion = currentSiloConfiguration.Region;
                    bool isReplica = true;

                    startId++;
                    startPort++;
                    startGatewayPort++;

                    SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, deployRegion, deployRegion, siloId, siloPort, gatewayPort, deployRegion, homeRegion, isReplica);

                    string stringKey = $"{homeRegion}-{deployRegion}";

                    replicas.Add(stringKey, siloInfo);
                }
            }

            return replicas;
        }

        private static void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration, Dictionary<string, SiloInfo> siloInfos)
        {

            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger=> Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");
            siloHostBuilder.ConfigureServices(serviceCollection => {
                serviceCollection.AddSingleton(globalConfiguration);
                RegionalConfiguration regionalConfiguration = new RegionalConfiguration();
                serviceCollection.AddSingleton(regionalConfiguration);
                serviceCollection.AddSingleton(siloInfos);
                serviceCollection.AddSingleton(logger);

                serviceCollection.AddSingleton<ICoordMap, CoordMap>();

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalConfigGrainPlacementStrategy>(nameof(RegionalConfigGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigGrainPlacement>(typeof(RegionalConfigGrainPlacementStrategy));
            });
        }

        private static void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder, Dictionary<string, SiloInfo> siloInfos, RegionalConfiguration regionalConfiguration)
        {
            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger=> Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");
            siloHostBuilder.ConfigureServices(services => 
            {
                services.AddSingleton(siloInfos);
                services.AddSingleton(regionalConfiguration);
                services.AddSingleton(logger);

                //services.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                //services.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacement));

                services.AddSingletonNamedService<PlacementStrategy, RegionalConfigGrainPlacementStrategy>(nameof(RegionalConfigGrainPlacementStrategy));
                services.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigGrainPlacement>(typeof(RegionalConfigGrainPlacementStrategy));

            });
        }

        private static void ConfigureLocalGrains(IServiceCollection services)
        {
            // all the singletons have one instance per silo host??
            services.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigGrainPlacement>(typeof(LocalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordGrainPlacement>(typeof(LocalCoordGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
        }
    }
}