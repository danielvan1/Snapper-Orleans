using System.Collections.ObjectModel;
using System.Net;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public class SiloConfigurationForGlobalDeployment
    {
        private readonly ISiloInfoFactory siloInfoFactory;

        public SiloConfigurationForGlobalDeployment(ISiloInfoFactory siloInfoFactory)
        {
            this.siloInfoFactory = siloInfoFactory ?? throw new ArgumentNullException(nameof(siloInfoFactory));
        }

        public GlobalConfiguration CreateGlobalCoordinatorConfiguration(Silos silos, string region)
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

            IPAddress regionIPAddress = IPAddress.Parse(globalSiloConfiguration.IPAddress);

            return this.siloInfoFactory.Create(regionIPAddress, siloConfigurations.ClusterId, siloConfigurations.ServiceId, globalSiloConfiguration.SiloId,
                                               globalSiloConfiguration.SiloPort, globalSiloConfiguration.GatewayPort, globalSiloConfiguration.Region, globalSiloConfiguration.Region, false);
        }

        public RegionalSiloPlacementInfo CreateRegionalSiloPlacementInfo(SiloConfigurations siloConfigurations, string region)
        {
            var regionalSilos = new Dictionary<string, SiloInfo>();

            foreach (SiloConfiguration siloConfiguration in siloConfigurations.Silos.RegionalSilos.Where(config => config.Region.Equals(region)))
            {
                bool isReplica = false;

                IPAddress siloIPAddress = IPAddress.Parse(siloConfiguration.IPAddress);

                SiloInfo siloInfo = this.siloInfoFactory.Create(siloIPAddress, siloConfigurations.ClusterId, siloConfigurations.ServiceId, siloConfiguration.SiloId,
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

        public LocalCoordinatorConfiguration CreateLocalCoordinatorConfigurationForMaster(IReadOnlyList<SiloConfiguration> localSilos)
        {
            Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets = this.PutEachSiloConfigurationInRegionBuckets(localSilos);
            IReadOnlyDictionary<string, List<string>> siloIdsPerRegion = this.CreateMasterSiloIds(siloConfigurationRegionBuckets);

            return new LocalCoordinatorConfiguration()
            {
                SiloIdPerRegion = siloIdsPerRegion
            };
        }

        // public LocalSiloPlacementInfo CreateLocalSiloPlacementInfo(SiloConfigurations siloConfigurations)
        // {
        // }

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

        private IReadOnlyDictionary<string, List<string>> CreateMasterSiloIds(Dictionary<string, List<SiloConfiguration>> siloConfigurationRegionBuckets)
        {
            var siloIdsPerRegion = new Dictionary<string, List<string>>();

            foreach ((string homeRegion, List<SiloConfiguration> configurations) in siloConfigurationRegionBuckets)
            {
                for (int i = 0; i < configurations.Count; i++)
                {
                    // string siloId = this.CreateSiloId(homeRegion, homeRegion, i);
                    string siloId = string.Empty;

                    if (!siloIdsPerRegion.TryGetValue(homeRegion, out List<string> siloIds))
                    {
                        siloIdsPerRegion.Add(homeRegion, siloIds = new List<string>());
                    }

                    siloIds.Add(siloId);
                }
            }

            return siloIdsPerRegion;
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

            return numberOfSilosPerRegion;
        }
    }
}