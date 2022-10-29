using System.Collections.Generic;

namespace Concurrency.Implementation
{
    public interface IIdHelper
    {
        List<string> GetLocalReplicaSiloIds(string localSiloId);

        List<string> GetRegionalReplicaSiloIds(string regionalSiloId);

        string UpdateDeploymentRegion(string newDeploymentRegion, string localSiloId);
    }
}