using System.Threading.Tasks;
using Orleans;

namespace Concurrency.Interface.Configuration
{
    public interface IRegionalCoordinatorConfigGrain : IGrainWithIntegerCompoundKey
    {
        Task InitializeRegionalCoordinators(string currentRegion);
    }
}