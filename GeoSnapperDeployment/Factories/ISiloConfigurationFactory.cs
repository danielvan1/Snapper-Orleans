using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public interface ISiloConfigurationFactory
    {
        GlobalConfiguration CreateGlobalConfiguration(Silos siloConfigurations);
        SiloInfo CreateGlobalSiloInfo(SiloConfigurations siloConfigurations);

        RegionalSiloPlacementInfo CreateRegionalSiloPlacementInfo(SiloConfigurations siloConfigurations);
        RegionalCoordinatorConfiguration CreateRegionalConfiguration(IReadOnlyList<SiloConfiguration> regionalSilos);

        LocalCoordinatorConfiguration CreateLocalCoordinatorConfigurationWithReplica(IReadOnlyList<SiloConfiguration> localSilos);
        LocalCoordinatorConfiguration CreateLocalCoordinatorConfigurationForMaster(IReadOnlyList<SiloConfiguration> localSilos);
        LocalSiloPlacementInfo CreateLocalSiloPlacementInfo(SiloConfigurations siloConfigurations);
    }
}