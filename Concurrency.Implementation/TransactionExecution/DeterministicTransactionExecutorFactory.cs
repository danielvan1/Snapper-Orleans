using Concurrency.Implementation.LoadBalancing;
using Concurrency.Implementation.TransactionExecution.Scheduler;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DeterministicTransactionExecutorFactory : IDeterministicTransactionExecutorFactory
    {
        private readonly ILogger<DeterministicTransactionExecutor> logger;
        private readonly ICoordinatorProvider coordinatorProvider;
        private readonly ITransactionSchedulerFactory transactionSchedulerFactory;

        public DeterministicTransactionExecutorFactory(ILogger<DeterministicTransactionExecutor> logger, ICoordinatorProvider coordinatorProvider, ITransactionSchedulerFactory transactionSchedulerFactory)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.coordinatorProvider = coordinatorProvider ?? throw new System.ArgumentNullException(nameof(coordinatorProvider));
            this.transactionSchedulerFactory = transactionSchedulerFactory ?? throw new System.ArgumentNullException(nameof(transactionSchedulerFactory));
        }

        public IDeterministicTransactionExecutor Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId)
        {
            ITransactionScheduler transactionScheduler = this.transactionSchedulerFactory.Create();

            return new DeterministicTransactionExecutor(this.logger, this.coordinatorProvider, transactionScheduler, grainFactory, grainReference, grainId);
        }
    }
}