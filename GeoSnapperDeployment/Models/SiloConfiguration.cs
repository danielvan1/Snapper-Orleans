namespace GeoSnapperDeployment.Models
{
    public record SiloConfiguration
    {
        public string Region {get; init;}

        public string IPAddress {get; init;}

        public int SiloId {get; init;}

        public int SiloPort {get; init;}

        public int GatewayPort {get; init;}
    }
}