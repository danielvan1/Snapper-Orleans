using System.Collections.Generic;

namespace Concurrency.Interface.Configuration
{
    public record LocalConfiguration
    {
        public IReadOnlyDictionary<string, List<string>> SiloKeysPerRegion {get; init;}
    }
}