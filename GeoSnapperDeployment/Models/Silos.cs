using System.Collections.ObjectModel;
using Concurrency.Interface;

namespace GeoSnapperDeployment.Models
{
    public record Silos
    {
        public IReadOnlyList<SiloConfiguration> LocalSilos {get; init;}

        public IReadOnlyList<SiloConfiguration> RegionalSilos {get; init;}

        public SiloConfiguration GlobalSilo {get; init;}

        public SiloConfiguration PrimarySilo {get; init;}
    }
}