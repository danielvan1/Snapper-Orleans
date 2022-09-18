using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;

namespace Concurrency.Interface.Coordinator
{
    public interface IRegionalCoordinatorGrain : IGrainWithIntegerCompoundKey
    {
        Task PassToken(BasicToken token);

        Task SpawnGlobalCoordGrain(IRegionalCoordinatorGrain neighbor);
    }
}