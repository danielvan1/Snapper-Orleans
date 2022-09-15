using System;
using Concurrency.Interface.Models;

namespace GeoSnapperDeployment 
{
    public class SiloInfoFactory : ISiloInfoFactory
    {
        public SiloInfo Create(string clusterId, string serviceId, int siloId, int siloPort, int gatewayPort, string region, string homeRegion, bool IsReplica)
        {
            return new SiloInfo()
            {
                ClusterId = clusterId,
                ServiceId = serviceId,
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