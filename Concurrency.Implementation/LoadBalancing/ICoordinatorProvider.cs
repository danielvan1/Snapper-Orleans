using System.Threading.Tasks;
using Orleans;

namespace Concurrency.Implementation.LoadBalancing
{
    public interface ICoordinatorProvider<T> where T : IGrain
    {
        T GetCoordinator(string region);
    }
}