using Concurrency.Interface.Coordinator;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.TransactionContextProvider
{
    public interface ITransactionContextProviderFactory
    {
        ITransactionContextProvider Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId, ILocalCoordinatorGrain localCoordinatorGrain, IRegionalCoordinatorGrain regionalCoordinatorGrain);
    }
}