using Concurrency.Implementation.LoadBalancing;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DeterministicTransactionExecutorFactory : IDeterministicTransactionExecutorFactory
    {
        private readonly ILogger<DeterministicTransactionExecutor> logger;
        private readonly ICoordinatorProvider coordinatorProvider;

        public DeterministicTransactionExecutorFactory(ILogger<DeterministicTransactionExecutor> logger, ICoordinatorProvider coordinatorProvider)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.coordinatorProvider = coordinatorProvider ?? throw new System.ArgumentNullException(nameof(coordinatorProvider));
        }

        public IDeterministicTransactionExecutor Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId)
        {
            return new DeterministicTransactionExecutor(this.logger, this.coordinatorProvider, grainFactory, grainReference, grainId);
        }
    }
}