using System.Net;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Orleans.Hosting;

namespace GeoSnapperDeployment
{
    public class LocalSiloDeployer
    {
        private readonly ISiloConfigurationFactory siloConfigurationFactory;

        public LocalSiloDeployer(ISiloConfigurationFactory siloConfigurationFactory)
        {
            this.siloConfigurationFactory = siloConfigurationFactory ?? throw new ArgumentNullException(nameof(siloConfigurationFactory));
        }

        public async Task<ISiloHost> DeployPrimarySilo(SiloConfigurations siloConfigurations)
        {
            SiloHostBuilder siloHostBuilder = new SiloHostBuilder();
            SiloConfiguration primarySiloConfiguration = siloConfigurations.Silos.PrimarySilo;
            IPEndPoint primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, siloConfigurations.Silos.PrimarySilo.SiloPort);

            SiloConfigurationManager.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                                      siloConfigurations.ClusterId,
                                                                      siloConfigurations.ServiceId,
                                                                      primarySiloEndpoint,
                                                                      primarySiloConfiguration.SiloPort,
                                                                      primarySiloConfiguration.GatewayPort);


            SiloInfo globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);
            RegionalSiloPlacementInfo regionalSilos = this.siloConfigurationFactory.CreateRegionalSiloPlacementInfo(siloConfigurations);
            LocalSiloPlacementInfo localSilos = this.siloConfigurationFactory.CreateLocalSiloPlacementInfo(siloConfigurations);

            SiloConfigurationManager.ConfigurePrimarySilo(siloHostBuilder, globalSiloInfo, regionalSilos, localSilos);

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

            SiloConfigurationManager.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                                      siloConfigurations.ClusterId,
                                                                      siloConfigurations.ServiceId,
                                                                      primarySiloEndpoint,
                                                                      globalSiloConfiguration.SiloPort,
                                                                      globalSiloConfiguration.GatewayPort);

            GlobalConfiguration globalConfiguration = this.siloConfigurationFactory.CreateGlobalConfiguration(siloConfigurations.Silos);

            var regions = this.GetRegions(siloConfigurations.Silos.LocalSilos);

            SiloInfo globalSiloInfo = this.siloConfigurationFactory.CreateGlobalSiloInfo(siloConfigurations);
            SiloConfigurationManager.ConfigureGlobalGrains(siloHostBuilder, globalConfiguration, globalSiloInfo, regions);

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

                SiloConfigurationManager.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                                          siloConfigurations.ClusterId,
                                                                          siloConfigurations.ServiceId,
                                                                          primarySiloEndpoint,
                                                                          siloConfiguration.SiloPort,
                                                                          siloConfiguration.GatewayPort);

                SiloConfigurationManager.ConfigureRegionalGrains(siloHostBuilder, regionalSilos, regionalConfiguration, localConfiguration, localSilos, regions);

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

                SiloConfigurationManager.ConfigureLocalGrains(siloHostBuilder, regionalSilos, localSiloInfo, regions);

                SiloConfigurationManager.ConfigureLocalDeploymentSiloHost(siloHostBuilder,
                                                                          siloInfo.ClusterId,
                                                                          siloInfo.ServiceId,
                                                                          primarySiloEndpoint,
                                                                          siloInfo.SiloPort,
                                                                          siloInfo.GatewayPort);

                var siloHost = siloHostBuilder.Build();

                startSiloTasks.Add(siloHost.StartAsync());

                Console.WriteLine($"Silo {(siloInfo.IsReplica ? "replica" : string.Empty)} {siloInfo.SiloId} in region {siloRegion} is started...");

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