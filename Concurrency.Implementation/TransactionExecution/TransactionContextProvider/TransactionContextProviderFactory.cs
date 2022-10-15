using Concurrency.Implementation.Coordinator;
using Concurrency.Implementation.TransactionExecution.TransactionPlacement;
using Concurrency.Interface.Coordinator;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.TransactionContextProvider
{
    public class TransactionContextProviderFactory : ITransactionContextProviderFactory
    {
        private readonly ILogger<TransactionContextProvider> logger;
        private readonly ICoordinatorProvider coordinatorProvider;
        private readonly IPlacementManager placementManager;

        public TransactionContextProviderFactory(ILogger<TransactionContextProvider> logger,
                                                 ICoordinatorProvider coordinatorProvider,
                                                 IPlacementManager placementManager)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.coordinatorProvider = coordinatorProvider ?? throw new System.ArgumentNullException(nameof(coordinatorProvider));
            this.placementManager = placementManager ?? throw new System.ArgumentNullException(nameof(placementManager));
        }

        public ITransactionContextProvider Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId, ILocalCoordinatorGrain localCoordinatorGrain, IRegionalCoordinatorGrain regionalCoordinatorGrain)
        {
            return new TransactionContextProvider(this.logger, this.coordinatorProvider, this.placementManager, grainFactory, localCoordinatorGrain, regionalCoordinatorGrain, grainReference, grainId);
        }
    }
}