using Orleans;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface IGlobalConfigurationGrain : IGrainWithIntegerKey
    {
        Task InitializeGlobalCoordinators();
    }
}