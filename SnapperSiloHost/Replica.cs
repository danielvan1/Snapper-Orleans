using System;

namespace SnapperSiloHost
{
    public class Replica
    {
        public string Region {get; set;}
        public string IPAddress {get; set;}
        public int Port {get; set;}

        public Replica(string region, int initialPort) {
            this.Region = region;
            this.Port = initialPort;
        }

        public Replica(string region, string ipAddress, int initialPort) {
            this.Region = region;
            this.IPAddress = ipAddress;
            this.Port = initialPort;
        }
    }
}