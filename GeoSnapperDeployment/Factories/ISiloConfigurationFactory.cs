using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using GeoSnapperDeployment.Models;

namespace GeoSnapperDeployment.Factories
{
    public interface ISiloConfigurationFactory
    {
        GlobalConfiguration CreateGlobalConfiguration(Silos siloConfigurations);

        RegionalSilosPlacementInfo CreateRegionalSilos(SiloConfigurations siloConfigurations);

        RegionalConfiguration CreateRegionalConfiguration(IReadOnlyList<SiloConfiguration> localSilos);

        LocalConfiguration CreateLocalConfiguration(IReadOnlyList<SiloConfiguration> localSilos);

        LocalSiloPlacementInfo CreateLocalSilosDictionary(SiloConfigurations siloConfigurations);

        SiloInfo CreateGlobalSiloInfo(SiloConfigurations siloConfigurations);
    }
}