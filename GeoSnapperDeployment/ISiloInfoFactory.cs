using Concurrency.Interface.Models;

namespace GeoSnapperDeployment 
{
    public interface ISiloInfoFactory
    {
        SiloInfo Create(string clusterId, string serviceId,  int siloId, int siloPort, int gatewayPort, string region, string homeRegion, bool IsReplica);
    }
}