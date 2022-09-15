namespace GeoSnapperDeployment.Models 
{
    public sealed class SiloConfiguration
    {
        public string ClusterId {get; init;} = string.Empty;

        public string ServiceId {get; init;} = string.Empty;

        public int SiloId {get; init;}

        public int SiloPort {get; init;}

        public int GatewayPort {get; init;}
    }
}