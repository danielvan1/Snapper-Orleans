using System.Collections.ObjectModel;
using System.Net;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public class SiloConfigurationForLocalDeploymentFactory : ISiloConfigurationFactory
    {
        private readonly ISiloInfoFactory siloInfoFactory;

        public SiloConfigurationForLocalDeploymentFactory(ISiloInfoFactory siloInfoFactory)
        {
            this.siloInfoFactory = siloInfoFactory ?? throw new ArgumentNullException(nameof(siloInfoFactory));
        }

        public GlobalConfiguration CreateGlobalConfiguration(Silos silos)
        {
            var regions = silos.RegionalSilos.Select(regionalSilo => regionalSilo.Region)
                                             .Distinct()
                                             .ToList();

            string deploymentRegion = silos.GlobalSilo.Region;

            return new GlobalConfiguration()
            {
                Regions = regions,
                DeploymentRegion = deploymentRegion
            };
        }

        public SiloInfo CreateGlobalSiloInfo(SiloConfigurations siloConfigurations)
        {
            SiloConfiguration globalSiloConfiguration = siloConfigurations.Silos.GlobalSilo;

            return this.siloInfoFactory.Create(IPAddress.Loopback, siloConfigurations.ClusterId, siloConfigurations.ServiceId, globalSiloConfiguration.SiloId,
                                               globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort, globalSiloConfiguration.Region, globalSiloConfiguration.Region, false);
        }

        public RegionalSiloPlacementInfo CreateRegionalSiloPlacementInfo(SiloConfigurations siloConfigurations)
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

            return new RegionalSiloPlacementInfo() { RegionsSiloInfo = regionalSilos };
        }

        public RegionalCoordinatorConfiguration CreateRegionalConfiguration(IReadOnlyList<SiloConfiguration> localSilos)
        {
            return new RegionalCoordinatorConfiguration()
            {
                NumberOfSilosPerRegion = this.GetNumberOfSilosPerRegion(localSilos)
            };
        }

        // Put each siloconfiguration in buckets of same regions.
        private Dictionary<string, List<SiloConfiguration>> PutEachSiloConfigurationInRegionBuckets(IReadOnlyList<SiloConfiguration> localSilos)
        {
            // Put each siloconfiguration in buckets of same regions.
            var siloConfigurationRegionBuckets = new Dictionary<string, List<SiloConfiguration>>();
            foreach (SiloConfiguration siloConfiguration in localSilos)
            {
                if (!siloConfigurationRegionBuckets.TryGetValue(siloConfiguration.Region, out List<SiloConfiguration> configurations))
                {
                    siloConfigurationRegionBuckets.Add(siloConfiguration.Region, configurations = new List<SiloConfiguration>());
                }

                configurations.Add(siloConfiguration);
            }

            return siloConfigurationRegionBuckets;
        }

        public LocalCoordinatorConfiguration CreateLocalCoordinatorConfigurationWithReplica(IReadOnlyList<SiloConfiguration> localSilos)
        {
            Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets = this.PutEachSiloConfigurationInRegionBuckets(localSilos);
            IReadOnlyDictionary<string, List<string>> siloKeysPerRegion = this.CreateSiloIds(siloConfigurationRegionBuckets);

            return new LocalCoordinatorConfiguration()
            {
                SiloIdPerRegion = siloKeysPerRegion
            };
        }

        public LocalCoordinatorConfiguration CreateLocalCoordinatorConfigurationForMaster(IReadOnlyList<SiloConfiguration> localSilos)
        {
            Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets = this.PutEachSiloConfigurationInRegionBuckets(localSilos);
            IReadOnlyDictionary<string, List<string>> siloIdsPerRegion = this.CreateMasterSiloIds(siloConfigurationRegionBuckets);

            return new LocalCoordinatorConfiguration()
            {
                SiloIdPerRegion = siloIdsPerRegion
            };
        }

        public LocalSiloPlacementInfo CreateLocalSiloPlacementInfo(SiloConfigurations siloConfigurations)
        {
            var localSilos = siloConfigurations.Silos.LocalSilos;

            Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets = this.PutEachSiloConfigurationInRegionBuckets(localSilos);
            Dictionary<string, SiloInfo> homeSilos = this.CreateHomeSiloInfos(siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfigurationRegionBuckets);
            Dictionary<string, SiloInfo> replicaSilos = this.CreateReplicaSiloInfos(siloConfigurations, siloConfigurationRegionBuckets);

            var homeAndReplicaSilos = this.MergeDictionaries(homeSilos, replicaSilos);

            return new LocalSiloPlacementInfo()
            {
                LocalSiloInfo = homeAndReplicaSilos
            };
        }

        private IReadOnlyDictionary<string, int> GetNumberOfSilosPerRegion(IReadOnlyList<SiloConfiguration> localSilos)
        {
            Dictionary<string, int> numberOfSilosPerRegion = new Dictionary<string, int>();

            foreach (var siloConfiguration in localSilos)
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

            return new ReadOnlyDictionary<string, int>(numberOfSilosPerRegion);
        }


        private IReadOnlyDictionary<string, List<string>> CreateMasterSiloIds(Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets)
        {
            var siloIdsPerRegion = new Dictionary<string, List<string>>();

            foreach ((string homeRegion, List<SiloConfiguration> configurations) in siloConfigurationRegionBuckets)
            {
                for (int i = 0; i < configurations.Count; i++)
                {
                    string siloId = this.CreateSiloId(homeRegion, homeRegion, i);

                    if (!siloIdsPerRegion.TryGetValue(homeRegion, out List<string> siloIds))
                    {
                        siloIdsPerRegion.Add(homeRegion, siloIds = new List<string>());
                    }

                    siloIds.Add(siloId);
                }
            }

            return siloIdsPerRegion;
        }

        private IReadOnlyDictionary<string, List<string>> CreateSiloIds(Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets)
        {
            var siloIdsPerRegion = new Dictionary<string, List<string>>();
            foreach ((string homeRegion, List<SiloConfiguration> configurations) in siloConfigurationRegionBuckets)
            {
                foreach ((string deploymentRegion, _) in siloConfigurationRegionBuckets)
                {
                    for (int i = 0; i < configurations.Count; i++)
                    {
                        string siloId = this.CreateSiloId(deploymentRegion, homeRegion, i);

                        if (!siloIdsPerRegion.TryGetValue(deploymentRegion, out List<string> siloIds))
                        {
                            siloIdsPerRegion.Add(deploymentRegion, siloIds = new List<string>());
                        }

                        siloIds.Add(siloId);
                    }
                }
            }
            return new ReadOnlyDictionary<string, List<string>>(siloIdsPerRegion);
        }

        private Dictionary<string, SiloInfo> CreateHomeSiloInfos(string clusterId, string serviceId, Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets)
        {
            var homeSilos = new Dictionary<string, SiloInfo>();

            foreach ((string region, List<SiloConfiguration> configurations) in siloConfigurationRegionBuckets)
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

                    string homeRegionAndServerKey = this.CreateSiloId(region, region, i);

                    homeSilos.Add(homeRegionAndServerKey, siloInfo);
                }
            }
            return homeSilos;
        }

        private  Dictionary<string, SiloInfo> CreateReplicaSiloInfos(SiloConfigurations siloConfigurations, Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets)
        {
            string clusterId = siloConfigurations.ClusterId;
            string serviceId = siloConfigurations.ServiceId;
            var replicaStartPort = siloConfigurations.ReplicaStartPort;
            var replicaStartGatewayPort = siloConfigurations.ReplicaStartGatewayPort;
            int replicaStartId = siloConfigurations.ReplicaStartId;

            var replicaSilos = new Dictionary<string, SiloInfo>();

            foreach ((string deploymentRegion, _) in siloConfigurationRegionBuckets)
            {
                foreach ((string homeRegion, List<SiloConfiguration> configurations) in siloConfigurationRegionBuckets)
                {
                    // We don't want to create home silo info here, we do that another place
                    if (deploymentRegion.Equals(homeRegion)) { continue; }

                    for (int i = 0; i < configurations.Count; i++)
                    {
                        SiloInfo siloInfo = this.siloInfoFactory.Create(IPAddress.Loopback,
                                                                        clusterId, serviceId,
                                                                        replicaStartId, replicaStartPort,
                                                                        replicaStartGatewayPort, deploymentRegion,
                                                                        homeRegion, true);

                        string regionAndServerKey = this.CreateSiloId(deploymentRegion, homeRegion, i);

                        replicaSilos.Add(regionAndServerKey, siloInfo);

                        replicaStartId++;
                        replicaStartPort++;
                        replicaStartGatewayPort++;
                    }
                }
            }

            return replicaSilos;
        }

        private Dictionary<string, SiloInfo> MergeDictionaries(Dictionary<string, SiloInfo> silos1, Dictionary<string, SiloInfo> silos2)
        {
            var result = new List<Dictionary<string, SiloInfo>>() {silos1, silos2};
            // if duplicate keys this throws an ArgumentException
            return result.SelectMany(x => x)
                    .ToDictionary(x => x.Key, y => y.Value);
        }

        private string CreateSiloId(string deploymentRegion, string homeRegion, int serverIndex)
        {
            return $"{deploymentRegion}-{homeRegion}-{serverIndex}";
        }
    }
}