using System.Collections.Generic;

namespace Concurrency.Interface.Configuration
{
    public record GlobalConfiguration
    {
        public IReadOnlyList<string> Regions {get; init;}
    }
}