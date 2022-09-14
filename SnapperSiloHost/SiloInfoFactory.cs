using System;
using Concurrency.Interface.Models;

namespace SnapperSiloHost
{
    public class SiloInfoFactory : ISiloInfoFactory
    {
        public SiloInfo Create(int siloId, int siloPort, int gatewayPort, string region, string homeRegion, bool IsReplica)
        {
            return new SiloInfo()
            {
                SiloId = siloId,
                SiloPort = siloPort,
                GatewayPort = gatewayPort,
                Region = region,
                HomeRegion = homeRegion,
                IsReplica = IsReplica
            };
        }
    }
}