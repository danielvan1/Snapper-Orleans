using System;
using Microsoft.Extensions.Logging;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public class TransactionSchedulerFactory : ITransactionSchedulerFactory
    {
        private readonly ILogger<TransactionScheduler> logger;
        private readonly ILogger<ScheduleInfoManager> scheduleInfoManagerLogger;

        public TransactionSchedulerFactory(ILogger<TransactionScheduler> logger, ILogger<ScheduleInfoManager> scheduleInfoManagerLogger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.scheduleInfoManagerLogger = scheduleInfoManagerLogger ?? throw new ArgumentNullException(nameof(scheduleInfoManagerLogger));
        }

        public ITransactionScheduler Create()
        {
            IScheduleInfoManager scheduleInfoManager = new ScheduleInfoManager(this.scheduleInfoManagerLogger);

            return new TransactionScheduler(logger, scheduleInfoManager);
        }
    }
}