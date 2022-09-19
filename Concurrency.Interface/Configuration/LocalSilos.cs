using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Interface.Configuration
{
    public record LocalSilos
    {
        public IReadOnlyDictionary<string, SiloInfo> LocalSiloInfo {get; init;}
    }
}