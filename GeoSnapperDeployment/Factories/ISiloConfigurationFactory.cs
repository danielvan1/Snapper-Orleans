using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public interface ISiloConfigurationFactory
    {
        GlobalConfiguration CreateGlobalConfiguration(SiloConfigurations siloConfigurations);

        RegionalSilosPlacementInfo CreateRegionalSilos(SiloConfigurations siloConfigurations);

        RegionalConfiguration CreateRegionalConfiguration(SiloConfigurations siloConfigurations);

        LocalConfiguration CreateLocalConfiguration(SiloConfigurations siloConfigurations);

        LocalSiloPlacementInfo CreateLocalSilosDictionary(SiloConfigurations siloConfigurations);

        SiloInfo CreateGlobalSiloInfo(SiloConfigurations siloConfigurations);
    }
}