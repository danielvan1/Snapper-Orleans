using System;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public class TransactionSchedulerFactory : ITransactionSchedulerFactory
    {
        private readonly ILogger<TransactionScheduler> logger;
        private readonly ILogger<ScheduleInfoManager> scheduleInfoManagerLogger;
        private readonly IScheduleInfoManagerFactory scheduleInfoManagerFactory;

        public TransactionSchedulerFactory(ILogger<TransactionScheduler> logger, ILogger<ScheduleInfoManager> scheduleInfoManagerLogger, IScheduleInfoManagerFactory scheduleInfoManagerFactory)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.scheduleInfoManagerLogger = scheduleInfoManagerLogger ?? throw new ArgumentNullException(nameof(scheduleInfoManagerLogger));
            this.scheduleInfoManagerFactory = scheduleInfoManagerFactory ?? throw new ArgumentNullException(nameof(scheduleInfoManagerFactory));
        }

        public ITransactionScheduler Create(GrainReference grainReference)
        {
            IScheduleInfoManager scheduleInfoManager = new ScheduleInfoManager(this.scheduleInfoManagerLogger, grainReference);

            return new TransactionScheduler(this.logger, this.scheduleInfoManagerFactory, grainReference);
        }
    }
}