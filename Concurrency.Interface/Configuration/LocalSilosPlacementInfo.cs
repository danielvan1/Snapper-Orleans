using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Interface.Configuration
{
    public record LocalSiloPlacementInfo
    {
        /// <summary>
        /// Contains a mapping such that we can get each local silo.
        /// The key in our case is EU-US-x, where x is some integer.
        /// The first region is the deployment region and the second is the home region.
        /// </summary>
        public IReadOnlyDictionary<string, SiloInfo> LocalSiloInfo {get; init;}
    }
}