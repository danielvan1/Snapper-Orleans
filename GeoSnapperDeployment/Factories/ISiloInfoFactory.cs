using System.Net;
using Concurrency.Interface.Models;

namespace GeoSnapperDeployment.Factories
{
    public interface ISiloInfoFactory
    {
        SiloInfo Create(IPAddress ipAddress, string clusterId, string serviceId,  int siloId,
                        int siloPort, int gatewayPort, string deploymentRegion, string homeRegion, bool IsReplica, int serverIndex = 0);
    }
}