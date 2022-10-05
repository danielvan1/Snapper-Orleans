using System;
using Concurrency.Implementation.LoadBalancing;
using Concurrency.Interface.Coordinator;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Concurrency.Implementation.Coordinator.Local
{
    public class LocalDeterministicTransactionProcessorFactory : ILocalDeterministicTransactionProcessorFactory
    {
        private readonly ILogger<LocalDeterministicTransactionProcessor> logger;
        private readonly ICoordinatorProvider coordinatorProvider;

        public LocalDeterministicTransactionProcessorFactory(ILogger<LocalDeterministicTransactionProcessor> logger,
                                                             ICoordinatorProvider coordinatorProvider)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.coordinatorProvider = coordinatorProvider ?? throw new ArgumentNullException(nameof(coordinatorProvider));
        }

        public ILocalDeterministicTransactionProcessor Create(IGrainFactory grainFactory, GrainReference grainReference)
        {
            return new LocalDeterministicTransactionProcessor(this.logger, this.coordinatorProvider, grainFactory, grainReference);
        }
    }
}