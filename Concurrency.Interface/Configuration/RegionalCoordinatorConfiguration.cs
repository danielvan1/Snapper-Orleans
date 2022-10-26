using System.Collections.Generic;
using MessagePack.Formatters;

namespace Concurrency.Interface.Configuration
{
    public class RegionalCoordinatorConfiguration
    {
        public IReadOnlyList<string> Regions {get; init;}

        public IReadOnlyDictionary<string, int> NumberOfSilosPerRegion {get; init;}
    }
}