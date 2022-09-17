using System.Net;
using Concurrency.Interface.Models;

namespace GeoSnapperDeployment 
{
    public interface ISiloInfoFactory
    {
        SiloInfo Create(IPAddress ipAddress, string clusterId, string serviceId,  int siloId, int siloPort, int gatewayPort, string region, string homeRegion, bool IsReplica);
    }
}