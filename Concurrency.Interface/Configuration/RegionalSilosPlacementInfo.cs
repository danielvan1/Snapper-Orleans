using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Interface.Configuration
{
    // Storing information to place grains in the regional Silo.
    public record RegionalSilosPlacementInfo
    {
        /// <summary>
        /// Mapping each region to the corresponding Silo.
        /// The key could for example be the region name.
        /// </summary>
        public Dictionary<string, SiloInfo> RegionsSiloInfo {get; init;}
    }
}