namespace GeoSnapperDeployment.Models
{
    public record SiloConfigurations
    {
        public int ReplicaStartGatewayPort {get; init;}

        public int ReplicaStartPort {get; init;}

        public int ReplicaStartId {get; init;}

        public string ClusterId {get; init;}

        public string ServiceId {get; init;}

        public Silos Silos {get; init;}
    }
}