using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Models;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Placement;
using SnapperSiloHost.Models;

namespace SnapperSiloHost
{
    public class DeployLocalDevelopmentEnvironment
    {
        private readonly ISiloInfoFactory siloInfoFactory;

        public DeployLocalDevelopmentEnvironment(ISiloInfoFactory siloInfoFactory)
        {
            this.siloInfoFactory = siloInfoFactory ?? throw new ArgumentNullException(nameof(siloInfoFactory));
        }

        public async Task<IList<ISiloHost>> DeploySilosAndReplicas(LocalDeployment localDeployment)
        {
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();
            var silos = localDeployment.LocalSilos;

            Dictionary<string, SiloInfo> replicas = CreateReplicasDictionary(silos, localDeployment.PrimarySiloEndpoint, localDeployment.StartGatewayPort);

            foreach((string siloRegion, SiloInfo siloInfo) in replicas)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, 
                                                     localDeployment.ClusterId, 
                                                     localDeployment.ServiceId, 
                                                     localDeployment.PrimarySiloEndpoint, 
                                                     siloInfo.SiloPort, siloInfo.GatewayPort);
                
                siloHostBuilder.ConfigureServices(s => 
                {
                    s.AddSingleton(replicas);
                });

                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} is started...");

                siloHosts.Add(siloHost);
            }

            await Task.WhenAll(startSiloTasks);

            return siloHosts;

        }

        public async Task<ISiloHost> DeployGlobalSilo(LocalDeployment localDeployment)
        {
            SiloConfiguration globalSiloInfo = localDeployment.GlobalSilo;
            var siloHostBuilder = new SiloHostBuilder();

            ConfigureLocalDeploymentSiloHost(siloHostBuilder, localDeployment.ClusterId, localDeployment.ServiceId,
                                            localDeployment.PrimarySiloEndpoint, globalSiloInfo.SiloPort, globalSiloInfo.GatewayPort);
            var siloHost = siloHostBuilder.Build();

            await siloHost.StartAsync();

            Console.WriteLine($"Global silo {globalSiloInfo.SiloId} is started");

            return siloHost;
        }

        private void ConfigureLocalDeploymentSiloHost(SiloHostBuilder siloHostBuilder, 
                                                     string clusterId,
                                                     string serviceId,
                                                     int localPrimarySiloEndpoint,
                                                     int siloPort,
                                                     int gatewayPort)
        {
            var primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, localPrimarySiloEndpoint);
            siloHostBuilder.UseDevelopmentClustering(primarySiloEndpoint)
                // The IP address used for clustering / to be advertised in membership tables
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback);

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

            siloHostBuilder.ConfigureServices(ConfigureGlobalCoordinator)
                           .ConfigureServices(ConfigureLocalGrains);

            siloHostBuilder.AddMemoryGrainStorageAsDefault();
        }


        private Dictionary<string, SiloInfo> CreateReplicasDictionary(IList<SiloConfiguration> silos, int startPort, int startGatewayPort)
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

                SiloInfo siloInfo = this.siloInfoFactory.Create(siloId, siloPort, gatewayPort, region, region, isReplica);

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

                    SiloInfo siloInfo = this.siloInfoFactory.Create(siloId, siloPort, gatewayPort, deployRegion, homeRegion, isReplica);

                    string stringKey = $"{homeRegion}-{deployRegion}";

                    replicas.Add(stringKey, siloInfo);
                }
            }

            return replicas;
        }

        private static void ConfigureGlobalCoordinator(IServiceCollection services)
        {
            services.AddSingleton<ILoggerGroup, LoggerGroup>();
            services.AddSingleton<ICoordMap, CoordMap>();

            services.AddSingletonNamedService<PlacementStrategy, GlobalConfigGrainPlacementStrategy>(nameof(GlobalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigGrainPlacement>(typeof(GlobalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));
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