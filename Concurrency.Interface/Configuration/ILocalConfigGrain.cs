using Orleans;
using System.Threading.Tasks;

namespace Concurrency.Interface.Configuration
{
    public interface ILocalConfigGrain : IGrainWithIntegerCompoundKey
    {
        Task InitializeLocalCoordinators(string currentRegion);
    }
}