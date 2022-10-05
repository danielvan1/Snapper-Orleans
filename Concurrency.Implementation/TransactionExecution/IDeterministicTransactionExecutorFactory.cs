using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution
{
    public interface IDeterministicTransactionExecutorFactory
    {
        IDeterministicTransactionExecutor Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId);
    }
}