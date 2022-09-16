using System.Collections.Generic;
using MessagePack.Formatters;

namespace Concurrency.Interface.Configuration
{
    public class RegionalConfiguration
    {
        public IReadOnlyList<string> Regions {get; init;}

        public IReadOnlyDictionary<string, int> NumberOfSilosInRegion {get; init;}
    }
}