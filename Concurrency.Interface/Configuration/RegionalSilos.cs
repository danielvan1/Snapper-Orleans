using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Interface.Configuration
{
    public class RegionalSilos
    {
        public Dictionary<string, SiloInfo> RegionsSiloInfo {get; init;}  
    }
}