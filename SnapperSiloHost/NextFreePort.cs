using System;

namespace SnapperSiloHost
{
    public record NextFreePort
    {
        public int Port {get; set;}
        public int GatewayPort {get; set;}
    }
}