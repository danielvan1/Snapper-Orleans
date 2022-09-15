namespace GeoSnapperDeployment.Models 
{
    public sealed class SiloConfigurations
    {
        public int StartGatewayPort {get; set;}

        public int PrimarySiloEndpoint {get; set;}

        public Silos Silos {get; init;}
    }
}