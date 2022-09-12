using System;

namespace SnapperSiloHost
{
    public class NextFreePort
    {
        public int Port {get; set;}
        public NextFreePort(int initialPort) {
            this.Port = initialPort;
        }
    }
}