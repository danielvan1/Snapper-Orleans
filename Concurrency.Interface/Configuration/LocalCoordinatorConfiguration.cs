using System.Collections.Generic;

namespace Concurrency.Interface.Configuration
{
    /// <summary>
    /// Record containing information for initializing the local coordinators.
    /// </summary>
    public record LocalCoordinatorConfiguration
    {
        /// <summary>
        /// The key is the region and the List contains the name of the Silo.
        /// An example of the name of the Silo is "EU-US-2".
        /// </summary>
        public IReadOnlyDictionary<string, List<string>> SiloIdPerRegion {get; init;}
    }
}