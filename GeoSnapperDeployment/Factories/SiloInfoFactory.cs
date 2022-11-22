using System.Net;
using Concurrency.Interface.Models;

namespace GeoSnapperDeployment.Factories
{
    public class SiloInfoFactory : ISiloInfoFactory
    {
        public SiloInfo Create(IPAddress ipAddress, string clusterId, string serviceId, int siloId, int siloPort, int gatewayPort, string region, string homeRegion, bool IsReplica, int serverIndex = 0)
        {
            IPEndPoint IPAddress = new IPEndPoint(ipAddress, siloPort);

            return new SiloInfo()
            {
                ClusterId = clusterId,
                ServiceId = serviceId,
                SiloId = siloId,
                SiloPort = siloPort,
                GatewayPort = gatewayPort,
                Region = region,
                HomeRegion = homeRegion,
                IsReplica = IsReplica,
                IPEndPoint = IPAddress,
                ServerIndex = serverIndex
            };
        }
    }
}