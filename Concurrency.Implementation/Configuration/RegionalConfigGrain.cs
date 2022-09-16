using Concurrency.Interface.Configuration;
using Orleans;

namespace Concurrency.Implementation.Configuration
{
    public class RegionalConfigGrain : Grain, IRegionalConfigGrain 
    {
        private readonly RegionalConfiguration regionalConfiguration;

        public RegionalConfigGrain(RegionalConfiguration regionalConfiguration)
        {
            this.regionalConfiguration = regionalConfiguration ?? throw new System.ArgumentNullException(nameof(regionalConfiguration));
        }
    }
}