using System.Dynamic;

namespace Concurrency.Interface.Models
{
    public record SiloInfo
    {
        public int SiloId {get; init;}

        public int SiloPort {get; init;}

        public int GatewayPort {get; init;}

        public string Region {get; set;} 

        public string HomeRegion {get; set;}

        public string SiloKey {get { return $"{this.HomeRegion}-{this.Region}"; }}

        public bool IsReplica {get; set;}
    }
}