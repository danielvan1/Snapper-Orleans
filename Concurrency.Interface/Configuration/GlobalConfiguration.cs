using System.Collections.Generic;

namespace Concurrency.Interface.Configuration
{
    public record GlobalConfiguration
    {
        public string DeploymentRegion {get; set;}

        public IReadOnlyList<string> Regions {get; init;}
    }
}