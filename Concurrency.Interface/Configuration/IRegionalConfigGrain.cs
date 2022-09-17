using System.Threading.Tasks;
using Orleans;

namespace Concurrency.Interface.Configuration
{
    public interface IRegionalConfigGrain : IGrainWithIntegerCompoundKey 
    {
        Task InitializeRegionalCoordinators(string currentRegion);
    }
}