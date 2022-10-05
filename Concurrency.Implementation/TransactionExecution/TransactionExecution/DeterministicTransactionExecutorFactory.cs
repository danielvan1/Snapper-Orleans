using Concurrency.Implementation.TransactionExecution.Scheduler;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.TransactionExecution
{
    public class DeterministicTransactionExecutorFactory : IDeterministicTransactionExecutorFactory
    {
        private readonly ILogger<DeterministicTransactionExecutor> logger;
        private readonly ITransactionSchedulerFactory transactionSchedulerFactory;

        public DeterministicTransactionExecutorFactory(ILogger<DeterministicTransactionExecutor> logger,
                                                       ITransactionSchedulerFactory transactionSchedulerFactory)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.transactionSchedulerFactory = transactionSchedulerFactory ?? throw new System.ArgumentNullException(nameof(transactionSchedulerFactory));
        }

        public IDeterministicTransactionExecutor Create(IGrainFactory grainFactory, GrainReference grainReference, GrainId grainId)
        {
            ITransactionScheduler transactionScheduler = this.transactionSchedulerFactory.Create();

            return new DeterministicTransactionExecutor(this.logger, transactionScheduler, grainFactory, grainReference, grainId);
        }
    }
}