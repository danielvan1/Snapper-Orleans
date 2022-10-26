using System.Net;
namespace GeoSnapperDeployment.Models
{
    public record SiloConfiguration
    {
        public string IPAddress {get; init;}

        public string Region {get; init;}

        public int SiloIntegerId {get; init;}

        public int SiloPort {get; init;}

        public int GatewayPort {get; init;}

    }
}