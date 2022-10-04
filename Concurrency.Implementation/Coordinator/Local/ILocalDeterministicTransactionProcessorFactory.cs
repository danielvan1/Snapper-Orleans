using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.Coordinator.Local
{
    public interface ILocalDeterministicTransactionProcessorFactory
    {
        ILocalDeterministicTransactionProcessor Create(IGrainFactory grainFactory, GrainReference grainReference);
    }
}