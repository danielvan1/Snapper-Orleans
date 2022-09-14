namespace Concurrency.Interface
{
    public sealed class SiloConfiguration
    {
        public int SiloId {get; init;}

        public int SiloPort {get; init;}

        public int GatewayPort {get; init;}

        public string Region {get; set;} 
    }
}