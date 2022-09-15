using Concurrency.Interface;

namespace GeoSnapperDeployment.Models
{
    public record Silos
    {
        public List<SiloConfiguration> LocalSilos {get; init;}

        public SiloConfiguration GlobalSilo {get; init;}
    }
}