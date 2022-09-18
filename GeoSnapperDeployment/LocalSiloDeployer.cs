using System.Collections.ObjectModel;
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
using Amazon.DynamoDBv2.Model;
using Microsoft.AspNetCore.Mvc.ModelBinding.Binders;

namespace GeoSnapperDeployment
{
    public class LocalSiloDeployer
    {
        private readonly ISiloInfoFactory siloInfoFactory;

        public LocalSiloDeployer(ISiloInfoFactory siloInfoFactory)
        {
            this.siloInfoFactory = siloInfoFactory ?? throw new ArgumentNullException(nameof(siloInfoFactory));
        }

        public async Task<ISiloHost> DeployPrimarySilo(SiloConfigurations siloConfigurations)
        {
            SiloHostBuilder siloHostBuilder = new SiloHostBuilder();
            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;

            IPEndPoint nullPrimarySiloEndpoint = null;

            ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, 
                                             siloConfigurations.ServiceId, nullPrimarySiloEndpoint,
                                             primarySiloConfiguration.SiloPort, primarySiloConfiguration.GatewayPort);


            // Configure global silo
            GlobalConfiguration globalConfiguration = this.CreateGlobalConfiguration(siloConfigurations);
            ConfigureGlobalGrains(siloHostBuilder, globalConfiguration);

            // Configure regional silos
            RegionalSilos regionalSilos = this.CreateRegionalSilos(siloConfigurations);
            RegionalConfiguration regionalConfiguration = this.CreateRegionalConfiguration(siloConfigurations);
            ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration);

            //TODO: Configure the local silos

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
            ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                             primarySiloEndpoint, globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort);
                                            
            GlobalConfiguration globalConfiguration = this.CreateGlobalConfiguration(siloConfigurations);

            ConfigureGlobalGrains(siloHostBuilder, globalConfiguration);

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

            RegionalSilos regionalSilos = this.CreateRegionalSilos(siloConfigurations);
            RegionalConfiguration regionalConfiguration = this.CreateRegionalConfiguration(siloConfigurations);

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            foreach(SiloConfiguration siloConfiguration in silos)
            {
                var siloHostBuilder = new SiloHostBuilder();

                ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                                 primarySiloEndpoint, siloConfiguration.SiloPort, siloConfiguration.GatewayPort);

                ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration);

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

            Dictionary<string, SiloInfo> replicas = CreateReplicasDictionary(silos, siloConfigurations.PrimarySiloEndpoint, siloConfigurations.StartGatewayPort);

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            foreach((string siloRegion, SiloInfo siloInfo) in replicas)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, 
                                                      siloInfo.ClusterId, 
                                                      siloInfo.ServiceId, 
                                                      primarySiloEndpoint, 
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


        private void ConfigureLocalDeploymentSiloHost(SiloHostBuilder siloHostBuilder, 
                                                      string clusterId,
                                                      string serviceId,
                                                      IPEndPoint primarySiloEndpoint,
                                                      int siloPort,
                                                      int gatewayPort)
        {
            siloHostBuilder.UseDevelopmentClustering(primarySiloEndpoint);
            siloHostBuilder.ConfigureEndpoints(IPAddress.Loopback, siloPort, gatewayPort)
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
                           });
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

        private GlobalConfiguration CreateGlobalConfiguration(SiloConfigurations siloConfigurations)
        {
            var regions = siloConfigurations.Silos.RegionalSilos.Select(regionalSilo => regionalSilo.Region)
                                                                .Distinct()
                                                                .ToList();

            return new GlobalConfiguration() 
            {
                Regions = regions
            };
        }

        private RegionalSilos CreateRegionalSilos(SiloConfigurations siloConfigurations)
        {
            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach(SiloConfiguration siloConfiguration in siloConfigurations.Silos.RegionalSilos)
            {
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfiguration.SiloId,
                                                                siloConfiguration.SiloPort, siloConfiguration.SiloPort, siloConfiguration.Region, 
                                                                siloConfiguration.Region, isReplica);

                regionalSilos.Add(siloConfiguration.Region, siloInfo);
            }

            return new RegionalSilos() { RegionsSiloInfo = regionalSilos };
        }

        private RegionalConfiguration CreateRegionalConfiguration(SiloConfigurations siloConfigurations)
        {
            Dictionary<string, int> numberOfSilosPerRegion = new Dictionary<string, int>();
            foreach(var siloConfiguration in siloConfigurations.Silos.LocalSilos)
            {
                string region = siloConfiguration.Region;
                if(numberOfSilosPerRegion.ContainsKey(region))
                {
                    numberOfSilosPerRegion[region]++;
                }
                else
                {
                    numberOfSilosPerRegion.Add(region, 1);
                }
            }

            return new RegionalConfiguration()
            {
                NumberOfSilosInRegion = new ReadOnlyDictionary<string, int>(numberOfSilosPerRegion)
            };
        }

        private static void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration)
        {

            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger=> Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");
            siloHostBuilder.ConfigureServices(serviceCollection => {
                serviceCollection.AddSingleton(globalConfiguration);
                serviceCollection.AddSingleton(logger);
                serviceCollection.AddSingleton<ICoordMap, CoordMap>();

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));
            });
        }

        private static void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder, RegionalSilos regionalSilos, RegionalConfiguration regionalConfiguration)
        {
            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger=> Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");
            siloHostBuilder.ConfigureServices(services => 
            {
                services.AddSingleton(regionalSilos);
                services.AddSingleton(regionalConfiguration);
                services.AddSingleton(logger);

                services.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                services.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacement));

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