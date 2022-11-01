using System.Net;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;

namespace GeoSnapperDeployment
{
    public class GlobalSiloDeployer
    {
        private readonly ISiloConfigurationForGlobalDeployment siloConfigurationFactory;

        public GlobalSiloDeployer(ISiloConfigurationForGlobalDeployment siloConfigurationFactory)
        {
            this.siloConfigurationFactory = siloConfigurationFactory ?? throw new ArgumentNullException(nameof(siloConfigurationFactory));
        }

        public async Task<List<ISiloHost>> Deploy(SiloConfigurations siloConfigurations, string region)
        {
            var regionals = await this.DeployRegionalSilos(siloConfigurations, region);
            var locals = await this.DeployLocalSilosAndReplicas(siloConfigurations, region);

            return regionals.Concat(locals)
                            .ToList();
        }

        public async Task<IList<ISiloHost>> DeployRegionalSilos(SiloConfigurations siloConfigurations, string region)
        {
            Console.WriteLine($"Regional silo configurations: {string.Join(" ,", siloConfigurations.Silos.RegionalSilos)}");
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();

            IEnumerable<SiloConfiguration> silosToDeploy = siloConfigurations.Silos.RegionalSilos.Where(config => config.Region.Equals(region));

            RegionalSiloPlacementInfo regionalPlacementInfo = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);
            RegionalCoordinatorConfiguration regionalCoordinatorConfiguration = this.siloConfigurationFactory.CreateRegionalConfiguration(siloConfigurations.Silos.LocalSilos);
            LocalCoordinatorConfiguration localCoordinatorConfiguration = this.siloConfigurationFactory.CreateLocalCoordinatorConfigurationForMaster(siloConfigurations.Silos.LocalSilos, region);
            LocalSiloPlacementInfo localPlacementInfo = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations, region);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            Console.WriteLine($"Starting to deploy region silo in region {region}. Regional silo configurations to deploy: {string.Join(" ,", silosToDeploy)}");

            foreach (SiloConfiguration regionalSiloConfiguration in silosToDeploy)
            {
                var siloHostBuilder = new SiloHostBuilder();

                IPAddress advertisedSiloIPAddress = IPAddress.Parse(regionalSiloConfiguration.IPAddress);

                SiloServiceConfigurationManager.ConfigureRegionalGrains(siloHostBuilder, regionalPlacementInfo, regionalCoordinatorConfiguration, localCoordinatorConfiguration, localPlacementInfo, regions);
                SiloServiceConfigurationManager.ConfigurePrimarySilo(siloHostBuilder, regionalPlacementInfo, localPlacementInfo);

                this.ConfigureSiloHost(siloHostBuilder,
                                       advertisedSiloIPAddress,
                                       siloConfigurations.ClusterId,
                                       siloConfigurations.ServiceId,
                                       regionalSiloConfiguration.SiloPort,
                                       regionalSiloConfiguration.GatewayPort);


                var siloHost = siloHostBuilder.Build();

                await siloHost.StartAsync();

                Console.WriteLine($"Silo regional {regionalSiloConfiguration.SiloIntegerId} in region {regionalSiloConfiguration.Region} is started...");

                siloHosts.Add(siloHost);
            }

            Console.WriteLine("Done with deploying regional silos");

            return siloHosts;
        }

        public async Task<IList<ISiloHost>> DeployLocalSilosAndReplicas(SiloConfigurations siloConfigurations, string region)
        {
            Console.WriteLine("Starting local silo deployment");
            var siloHosts = new List<ISiloHost>();
            var startSiloTasks = new List<Task>();

            IReadOnlyCollection<SiloConfiguration> silos = siloConfigurations.Silos.LocalSilos;

            LocalSiloPlacementInfo localSiloPlacementInfo = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations, region);
            RegionalSiloPlacementInfo regionalSiloPlacementInfo = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);
            Console.WriteLine($"LocalSiloInfos: {string.Join(", ", localSiloPlacementInfo.LocalSiloInfo.Keys)}");

            foreach ((string siloId, SiloInfo siloInfo) in localSiloPlacementInfo.LocalSiloInfo.Where(kv => kv.Key.Substring(0,region.Length).Equals(region)))
            {
                IPAddress advertisedSiloIPAddress = siloInfo.IPEndPoint.Address;
                Console.WriteLine($"Deploying local silo with int id: {siloInfo.SiloId}");

                var siloHostBuilder = new SiloHostBuilder();

                SiloServiceConfigurationManager.ConfigureLocalGrains(siloHostBuilder, regionalSiloPlacementInfo, localSiloPlacementInfo, regions);
                SiloServiceConfigurationManager.ConfigurePrimarySilo(siloHostBuilder, regionalSiloPlacementInfo, localSiloPlacementInfo);

                this.ConfigureSiloHost(siloHostBuilder,
                                       advertisedSiloIPAddress,
                                       siloInfo.ClusterId,
                                       siloInfo.ServiceId,
                                       siloInfo.SiloPort,
                                       siloInfo.GatewayPort);

                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} in region {siloId} is started...");

                siloHosts.Add(siloHost);
            }

            await Task.WhenAll(startSiloTasks);

            return siloHosts;
        }

        private void ConfigureSiloHost(SiloHostBuilder siloHostBuilder,
                                       IPAddress advertisedSiloIPAdress,
                                       string clusterId,
                                       string serviceId,
                                       int siloPort,
                                       int gatewayPort)
        {
            const string key1 = "DefaultEndpointsProtocol=https;AccountName=snapperstorage;AccountKey=OYoqvb955xUGAu9SkZEMapbNAxl3vN3En2wNqVQV6iEmZE4UWCydMFL/cO+78QvN0ufhxWZNlZIA+AStQx1IXQ==;EndpointSuffix=core.windows.net";
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

        private List<string> GetRegions(IReadOnlyList<SiloConfiguration> localSilos)
        {
            return localSilos.Select(localSilo => localSilo.Region)
                             .Distinct()
                             .ToList();
        }
    }
}