using System.Collections.ObjectModel;
using System.Net;
using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyModel.Resolution;
using Microsoft.Extensions.Logging;
using Orleans;
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

        public async Task<ISiloHost> DeployPrimarySilo(SiloConfigurations siloConfigurations)
        {
            SiloHostBuilder siloHostBuilder = new SiloHostBuilder();
            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;

            IPEndPoint nullPrimarySiloEndpoint = null;

            this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId,
                                                  siloConfigurations.ServiceId, nullPrimarySiloEndpoint,
                                                  primarySiloConfiguration.SiloPort, primarySiloConfiguration.GatewayPort);


            // Configure global silo
            GlobalConfiguration globalConfiguration = this.CreateGlobalConfiguration(siloConfigurations);
            this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration);

            // Configure regional silos
            RegionalSilos regionalSilos = this.CreateRegionalSilos(siloConfigurations);
            RegionalConfiguration regionalConfiguration = this.CreateRegionalConfiguration(siloConfigurations);
            LocalConfiguration localConfiguration = this.CreateLocalConfiguration(siloConfigurations);
            var localSiloInfo = CreateLocalSilosDictionary(siloConfigurations);
            this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSiloInfo);

            // Configure the local silos
            this.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo);

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

            GlobalConfiguration globalConfiguration = this.CreateGlobalConfiguration(siloConfigurations);

            this.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration);

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
            LocalConfiguration localConfiguration = this.CreateLocalConfiguration(siloConfigurations);
            LocalSilos localSilos = this.CreateLocalSilosDictionary(siloConfigurations);

            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            foreach (SiloConfiguration siloConfiguration in silos)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalDeploymentSiloHost(siloHostBuilder, siloConfigurations.ClusterId, siloConfigurations.ServiceId,
                                                      primarySiloEndpoint, siloConfiguration.SiloPort, siloConfiguration.GatewayPort);

                this.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSilos);

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
            LocalSilos localSiloInfo = this.CreateLocalSilosDictionary(siloConfigurations);
            RegionalSilos regionalSilos = this.CreateRegionalSilos(siloConfigurations);

            foreach ((string siloRegion, SiloInfo siloInfo) in localSiloInfo.LocalSiloInfo)
            {
                var siloHostBuilder = new SiloHostBuilder();

                this.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo);

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

        private LocalConfiguration CreateLocalConfiguration(SiloConfigurations siloConfigurations)
        {
            var localSilos = siloConfigurations.Silos.LocalSilos;

            var siloConfigurationBuckets = new Dictionary<string, List<SiloConfiguration>>();
            var siloKeysPerRegion = new Dictionary<string, List<string>>();

            // Put each siloconfiguration in buckets of same regions.
            foreach (SiloConfiguration siloConfiguration in localSilos)
            {
                if (!siloConfigurationBuckets.TryGetValue(siloConfiguration.Region, out List<SiloConfiguration> configurations))
                {
                    siloConfigurationBuckets.Add(siloConfiguration.Region, configurations = new List<SiloConfiguration>());
                }

                configurations.Add(siloConfiguration);
            }

            foreach ((string homeRegion, _) in siloConfigurationBuckets)
            {
                foreach ((string deploymentRegion, List<SiloConfiguration> configurations) in siloConfigurationBuckets)
                {
                    for (int i = 0; i < configurations.Count; i++)
                    {
                        string siloKey = $"{deploymentRegion}-{homeRegion}-{i}";

                        if (!siloKeysPerRegion.TryGetValue(deploymentRegion, out List<string> siloKeys))
                        {
                            siloKeysPerRegion.Add(deploymentRegion, siloKeys = new List<string>());
                        }

                        siloKeys.Add(siloKey);
                    }
                }
            }

            return new LocalConfiguration()
            {
                SiloKeysPerRegion = siloKeysPerRegion
            };
        }

        private LocalSilos CreateLocalSilosDictionary(SiloConfigurations siloConfigurations)
        {
            var silos = new Dictionary<string, SiloInfo>();
            var localSilos = siloConfigurations.Silos.LocalSilos;
            string clusterId = siloConfigurations.ClusterId;
            string serviceId = siloConfigurations.ServiceId;
            Dictionary<string, List<SiloConfiguration>> siloConfigurationBuckets = new Dictionary<string, List<SiloConfiguration>>();

            // Put each siloconfiguration in buckets of same regions.
            foreach (SiloConfiguration siloConfiguration in localSilos)
            {
                if (!siloConfigurationBuckets.TryGetValue(siloConfiguration.Region, out List<SiloConfiguration> configurations))
                {
                    siloConfigurationBuckets.Add(siloConfiguration.Region, configurations = new List<SiloConfiguration>());
                }

                configurations.Add(siloConfiguration);
            }

            // main silos
            foreach ((string region, List<SiloConfiguration> configurations) in siloConfigurationBuckets)
            {
                for (int i = 0; i < configurations.Count; i++)
                {
                    var siloConfiguration = configurations[i];

                    int siloId = siloConfiguration.SiloId;
                    int siloPort = siloConfiguration.SiloPort;
                    int gatewayPort = siloConfiguration.GatewayPort;
                    bool isReplica = false;

                    SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, clusterId, serviceId, siloId,
                                                                    siloPort, gatewayPort, region, region, isReplica);

                    string stringKey = $"{region}-{region}-{i}";

                    silos.Add(stringKey, siloInfo);
                }
            }

            var startPort = siloConfigurations.ReplicaStartPort;
            var startGatewayPort = siloConfigurations.ReplicaStartGatewayPort;
            int startId = siloConfigurations.ReplicaStartId;

            // replica silos
            foreach ((string deploymentRegion, _) in siloConfigurationBuckets)
            {
                foreach ((string homeRegion, List<SiloConfiguration> configurations) in siloConfigurationBuckets)
                {
                    if (deploymentRegion.Equals(homeRegion)) continue;

                    for (int i = 0; i < configurations.Count; i++)
                    {
                        var configuration = configurations[i];

                        int siloId = startId;
                        int siloPort = startPort;
                        int gatewayPort = startGatewayPort;
                        bool isReplica = true;

                        SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, clusterId, serviceId, siloId, siloPort,
                                                                        gatewayPort, deploymentRegion, homeRegion, isReplica);

                        string stringKey = $"{deploymentRegion}-{homeRegion}-{i}";

                        silos.Add(stringKey, siloInfo);

                        startPort++;
                        startGatewayPort++;
                        startId++;
                    }
                }
            }

            return new LocalSilos()
            {
                LocalSiloInfo = silos
            };
        }

        private GlobalConfiguration CreateGlobalConfiguration(SiloConfigurations siloConfigurations)
        {
            var regions = siloConfigurations.Silos.RegionalSilos.Select(regionalSilo => regionalSilo.Region)
                                                                .Distinct()
                                                                .ToList();

            string deploymentRegion = siloConfigurations.Silos.GlobalSilo.Region;

            return new GlobalConfiguration()
            {
                Regions = regions,
                DeploymentRegion = deploymentRegion
            };
        }

        private RegionalSilos CreateRegionalSilos(SiloConfigurations siloConfigurations)
        {
            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach (SiloConfiguration siloConfiguration in siloConfigurations.Silos.RegionalSilos)
            {
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfiguration.SiloId,
                                                                siloConfiguration.SiloPort, siloConfiguration.SiloPort, siloConfiguration.Region,
                                                                siloConfiguration.Region, isReplica);

                string regionalKey = $"{siloConfiguration.Region}-Regional";
                string localKey = $"{siloConfiguration.Region}-Local";
                regionalSilos.Add(regionalKey, siloInfo);
                regionalSilos.Add(localKey, siloInfo);
            }

            return new RegionalSilos() { RegionsSiloInfo = regionalSilos };
        }

        private RegionalConfiguration CreateRegionalConfiguration(SiloConfigurations siloConfigurations)
        {
            Dictionary<string, int> numberOfSilosPerRegion = new Dictionary<string, int>();

            foreach (var siloConfiguration in siloConfigurations.Silos.LocalSilos)
            {
                string region = siloConfiguration.Region;
                if (numberOfSilosPerRegion.ContainsKey(region))
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

        private void ConfigureGlobalGrains(SiloHostBuilder siloHostBuilder, GlobalConfiguration globalConfiguration)
        {

            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger => Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");
            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddSingleton(globalConfiguration);
                serviceCollection.AddSingleton(logger);

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalConfigurationGrainPlacementStrategy>(nameof(GlobalConfigurationGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigurationGrainPlacement>(typeof(GlobalConfigurationGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));
            });
        }

        private void ConfigureRegionalGrains(SiloHostBuilder siloHostBuilder,
                                             RegionalSilos regionalSilos,
                                             RegionalConfiguration regionalConfiguration,
                                             LocalConfiguration localConfiguration,
                                             LocalSilos localSilos)
        {
            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger => Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");

            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(regionalConfiguration);
                serviceCollection.AddSingleton(localConfiguration);
                serviceCollection.AddSingleton(logger);
                serviceCollection.AddSingleton(localSilos);

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalCoordinatorGrainPlacementStrategy>(nameof(RegionalCoordinatorGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalCoordinatorGrainPlacement>(typeof(RegionalCoordinatorGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, RegionalConfigGrainPlacementStrategy>(nameof(RegionalConfigGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, RegionalConfigGrainPlacement>(typeof(RegionalConfigGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalConfigGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigGrainPlacement>(typeof(LocalConfigGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalCoordGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordGrainPlacement>(typeof(LocalCoordGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
            });
        }

        private void ConfigureLocalGrains(SiloHostBuilder siloHostBuilder, RegionalSilos regionalSilos, LocalSilos localSilos)
        {
            ILoggerFactory loggerFactory = LoggerFactory.Create(Logger => Logger.AddConsole());
            ILogger logger = loggerFactory.CreateLogger("smt");

            siloHostBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddSingleton(regionalSilos);
                serviceCollection.AddSingleton(localSilos);
                serviceCollection.AddSingleton(logger);

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalConfigGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigGrainPlacement>(typeof(LocalConfigGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalCoordGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordGrainPlacement>(typeof(LocalCoordGrainPlacementStrategy));

                serviceCollection.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
                serviceCollection.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));
            });
        }
    }
}