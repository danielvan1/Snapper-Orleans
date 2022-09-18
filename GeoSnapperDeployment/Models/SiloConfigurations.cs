namespace GeoSnapperDeployment.Models
{
    public sealed class SiloConfigurations
    {
        public int StartGatewayPort {get; set;}

        public int PrimarySiloEndpoint {get; set;}

        public string ClusterId {get; set;}

        public string ServiceId {get; set;}

        public Silos Silos {get; init;}
    }
}