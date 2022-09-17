using System;
using System.Threading.Tasks;
using Concurrency.Interface.Configuration;
using Orleans;

namespace Concurrency.Implementation.Configuration
{
    public class RegionalConfigGrain : Grain, IRegionalConfigGrain
    {
        private readonly RegionalConfiguration regionalConfiguration;

        public RegionalConfigGrain(RegionalConfiguration regionalConfiguration)
        {
            this.regionalConfiguration = regionalConfiguration ?? throw new ArgumentNullException(nameof(regionalConfiguration));
        }

        public async Task RegionalCoordinators(string currentRegion)
        {

        }
    }
}