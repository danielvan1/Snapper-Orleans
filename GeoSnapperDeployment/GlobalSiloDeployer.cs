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

            Console.WriteLine($"");
            Console.WriteLine($"Starting to deploy region silo in region {region}. Regional silo configurations to deploy: {string.Join(" ,", silosToDeploy)}");

            foreach (SiloConfiguration regionalSiloConfiguration in silosToDeploy)
            {
                var siloHostBuilder = new SiloHostBuilder();

                IPAddress advertisedSiloIPAddress = IPAddress.Parse(regionalSiloConfiguration.IPAddress);

                SiloConfigurationManager.ConfigureGlobalDeploymentRegionalGrains(siloHostBuilder, regionalPlacementInfo, regionalCoordinatorConfiguration, localCoordinatorConfiguration, localPlacementInfo, regions);
                SiloConfigurationManager.ConfigureGlobalDeploymentPrimarySilo(siloHostBuilder, regionalPlacementInfo, localPlacementInfo);

                SiloConfigurationManager.ConfigureGlobalDeploymentSiloHost(siloHostBuilder,
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

                SiloConfigurationManager.ConfigureGlobalDeploymentLocalGrains(siloHostBuilder, regionalSiloPlacementInfo, localSiloPlacementInfo, regions);
                SiloConfigurationManager.ConfigureGlobalDeploymentPrimarySilo(siloHostBuilder, regionalSiloPlacementInfo, localSiloPlacementInfo);

                SiloConfigurationManager.ConfigureGlobalDeploymentSiloHost(siloHostBuilder,
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

        private List<string> GetRegions(IReadOnlyList<SiloConfiguration> localSilos)
        {
            return localSilos.Select(localSilo => localSilo.Region)
                             .Distinct()
                             .ToList();
        }
    }
}