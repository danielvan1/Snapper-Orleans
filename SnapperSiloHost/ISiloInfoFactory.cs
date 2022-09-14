using Concurrency.Interface.Models;

namespace SnapperSiloHost
{
    public interface ISiloInfoFactory
    {
        SiloInfo Create(int siloId, int siloPort, int gatewayPort, string region, string homeRegion, bool IsReplica);
    }
}