using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

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
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            IReadOnlyList<SiloConfiguration> silos = siloConfigurations.Silos.RegionalSilos;

            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach(SiloConfiguration siloConfiguration in silos)
            {
                bool isReplica = false;

                var siloInfo = this.siloInfoFactory.Create(siloConfiguration.ClusterId, siloConfiguration.ServiceId, siloConfiguration.SiloId,
                                                           siloConfiguration.SiloPort, siloConfiguration.SiloPort, siloConfiguration.ClusterId, 
                                                           siloConfiguration.ClusterId, isReplica);

                regionalSilos.Add(siloConfiguration.ClusterId, siloInfo);
            }

            foreach(SiloConfiguration siloConfiguration in silos)
            {
                var siloHostBuilder = new SiloHostBuilder();

                ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfiguration.ClusterId, siloConfiguration.ServiceId,
                                                 siloConfigurations.PrimarySiloEndpoint, siloConfiguration.SiloPort, siloConfiguration.GatewayPort);

                siloHostBuilder.ConfigureServices(serviceCollection => serviceCollection.AddSingleton(regionalSilos));
                siloHostBuilder.ConfigureServices(ConfigureRegionalGrains);
                
                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo regional {siloConfiguration.SiloId} in region {siloConfiguration.ClusterId} is started...");

                siloHosts.Add(siloHost);
            }

            await Task.WhenAll(startSiloTasks);

            return siloHosts;
        }

        public async Task<ISiloHost> DeployGlobalSilo(SiloConfigurations siloConfigurations)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;
            var siloHostBuilder = new SiloHostBuilder();

            ConfigureLocalDeploymentSiloHost(siloHostBuilder, globalSiloConfiguration.ClusterId, globalSiloConfiguration.ServiceId,
                                             siloConfigurations.PrimarySiloEndpoint, globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort);
                                            

            var regions = siloConfigurations.Silos.RegionalSilos.Select(regionalSilo => regionalSilo.ClusterId).ToList();
            GlobalConfiguration globalConfiguration = new GlobalConfiguration() 
            {
                Regions = regions
            };

            ConfigureGlobalGrains(siloHostBuilder, globalConfiguration);

            var siloHost = siloHostBuilder.Build();

            await siloHost.StartAsync();

            Console.WriteLine($"Global silo {globalSiloConfiguration.SiloId} in region {globalSiloConfiguration.ClusterId} is started");

            return siloHost;
        }

        private void ConfigureLocalDeploymentSiloHost(SiloHostBuilder siloHostBuilder, 
                                                      string clusterId,
                                                      string serviceId,
                                                      int localPrimarySiloEndpoint,
                                                      int siloPort,
                                                      int gatewayPort)
        {
            siloHostBuilder.UseLocalhostClustering();

            siloHostBuilder.Configure<ClusterOptions>(options =>
            {
                options.ClusterId = clusterId;
                options.ServiceId = serviceId;
            });

            siloHostBuilder.Configure<EndpointOptions>(options =>
            {
                options.SiloPort = siloPort;
                options.GatewayPort = gatewayPort;
            });

            siloHostBuilder.AddMemoryGrainStorageAsDefault();
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
                string region = siloConfiguration.ClusterId;
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(region, region, siloId, siloPort, gatewayPort, region, region, isReplica);

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
                    string deployRegion = replicaSiloConfiguration.ClusterId;
                    string homeRegion = currentSiloConfiguration.ClusterId;
                    bool isReplica = true;

                    startId++;
                    startPort++;
                    startGatewayPort++;

                    SiloInfo siloInfo = this.siloInfoFactory.Create(deployRegion, deployRegion, siloId, siloPort, gatewayPort, deployRegion, homeRegion, isReplica);

                    string stringKey = $"{homeRegion}-{deployRegion}";

                    replicas.Add(stringKey, siloInfo);
                }
            }

            return replicas;
        }

        private static void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration)
        {
            siloHostBuilder.ConfigureServices(serviceCollection => {

                serviceCollection.AddSingleton(globalConfiguration);

                serviceCollection.AddSingleton<ILoggerGroup, LoggerGroup>();
                serviceCollection.AddSingleton<ICoordMap, CoordMap>();

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));
            });
        }

        private static void ConfigureRegionalGrains(IServiceCollection services)
        {
            services.AddSingletonNamedService<PlacementStrategy, RegionalConfigGrainPlacementStrategy>(nameof(RegionalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigGrainPlacement>(typeof(RegionalConfigGrainPlacement));

            services.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacement));
        }

        private static void ConfigureLocalGrains(IServiceCollection services)
        {
            // all the singletons have one instance per silo host??
            services.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigGrainPlacement>(typeof(LocalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, LocalCoordGrainPlacementStrategy>(nameof(LocalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordGrainPlacement>(typeof(LocalCoordGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
        }
    }
}