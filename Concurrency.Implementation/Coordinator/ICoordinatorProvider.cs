using Concurrency.Interface.Coordinator;
using Orleans;

namespace Concurrency.Implementation.Coordinator
{
    public interface ICoordinatorProvider
    {
        IRegionalCoordinatorGrain GetRegionalCoordinator(int id, string region, IGrainFactory grainFactory);

        ILocalCoordinatorGrain GetLocalCoordinatorGrain(int id, string region, IGrainFactory grainFactory);
    }
}