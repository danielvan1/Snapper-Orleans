using System.Collections.ObjectModel;
using System.Net;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public class SiloConfigurationFactory : ISiloConfigurationFactory
    {
        private readonly ISiloInfoFactory siloInfoFactory;

        public SiloConfigurationFactory(ISiloInfoFactory siloInfoFactory)
        {
            this.siloInfoFactory = siloInfoFactory ?? throw new ArgumentNullException(nameof(siloInfoFactory));
        }

        public LocalConfiguration CreateLocalConfiguration(SiloConfigurations siloConfigurations)
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

            foreach ((string homeRegion, List<SiloConfiguration> configurations) in siloConfigurationBuckets)
            {
                foreach ((string deploymentRegion, _) in siloConfigurationBuckets)
                {
                    for (int i = 0; i < configurations.Count; i++)
                    {
                        string siloKey = $"{deploymentRegion}-{homeRegion}-{i}";
                        Console.WriteLine(siloKey);

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

        public LocalSiloPlacementInfo CreateLocalSilosDictionary(SiloConfigurations siloConfigurations)
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

            return new LocalSiloPlacementInfo()
            {
                LocalSiloInfo = silos
            };
        }

        public GlobalConfiguration CreateGlobalConfiguration(SiloConfigurations siloConfigurations)
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

        public RegionalSilosPlacementInfo CreateRegionalSilos(SiloConfigurations siloConfigurations)
        {
            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach (SiloConfiguration siloConfiguration in siloConfigurations.Silos.RegionalSilos)
            {
                bool isReplica = false;

                SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfiguration.SiloId,
                                                                siloConfiguration.SiloPort, siloConfiguration.SiloPort, siloConfiguration.Region,
                                                                siloConfiguration.Region, isReplica);

                regionalSilos.Add(siloConfiguration.Region, siloInfo);
            }

            return new RegionalSilosPlacementInfo() { RegionsSiloInfo = regionalSilos };
        }

        public RegionalConfiguration CreateRegionalConfiguration(SiloConfigurations siloConfigurations)
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

        public SiloInfo CreateGlobalSiloInfo(SiloConfigurations siloConfigurations)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;

            return this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, globalSiloConfiguration.SiloId,
                                               globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort, globalSiloConfiguration.Region, globalSiloConfiguration.Region, false);
        }
    }
}