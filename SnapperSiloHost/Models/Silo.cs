namespace SnapperSiloHost.Models
{
    public sealed class SiloInfo
    {
        public int SiloId {get; set;}

        public int SiloPort {get; set;}

        public int GatewayPort {get; set;}

        public string Region {get; set;} 
    }
}