using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.TransactionContextProvider
{
    public interface ITransactionContextProviderFactory
    {
        ITransactionContextProvider Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId);
    }
}