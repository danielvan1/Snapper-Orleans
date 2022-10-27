using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public interface ISiloConfigurationForGlobalDeployment
    {
        GlobalConfiguration CreateGlobalCoordinatorConfiguration(Silos silos);
        SiloInfo CreateGlobalSiloInfo(SiloConfigurations siloConfigurations);
        RegionalCoordinatorConfiguration CreateRegionalConfiguration(IReadOnlyList<SiloConfiguration> localSilos);
        RegionalSiloPlacementInfo CreateRegionalSiloPlacementInfo(SiloConfigurations siloConfigurations);
        LocalCoordinatorConfiguration CreateLocalCoordinatorConfigurationForMaster(IReadOnlyList<SiloConfiguration> localSilos);
        LocalSiloPlacementInfo CreateLocalSiloPlacementInfo(SiloConfigurations siloConfigurations, string region);
    }
}