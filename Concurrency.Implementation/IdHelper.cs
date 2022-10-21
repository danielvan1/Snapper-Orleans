using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Concurrency.Implementation
{
    public class IdHelper : IIdHelper
    {
        private readonly ILogger<IdHelper> logger;
        private readonly List<string> regions;

        public IdHelper(ILogger<IdHelper> logger, List<string> regions)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.regions = regions ?? throw new System.ArgumentNullException(nameof(regions));
        }

        public List<string> GetLocalReplicaSiloIds(string localSiloId)
        {
            var updatedDeploymentRegions = new List<string>();
            string currentRegion = localSiloId.Substring(0, 2);

            foreach(string region in this.regions)
            {
                if(region == localSiloId) continue;

                updatedDeploymentRegions.Add(this.UpdateDeploymentRegion(region, localSiloId));
            }

            return updatedDeploymentRegions;
        }

        public List<string> GetRegionalReplicaSiloIds(string regionalSiloId)
        {
            return this.regions.Where(region => !region.Equals(regionalSiloId)).ToList();
        }

        public string UpdateDeploymentRegion(string newDeploymentRegion, string localSiloId)
        {
            return $"{newDeploymentRegion}-{localSiloId.Substring(3)}";
        }
    }
}