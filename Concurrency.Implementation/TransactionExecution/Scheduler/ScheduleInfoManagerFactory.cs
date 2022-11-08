using System;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public class ScheduleInfoManagerFactory : IScheduleInfoManagerFactory
    {
        private readonly ILogger<ScheduleInfoManager> logger;

        public ScheduleInfoManagerFactory(ILogger<ScheduleInfoManager> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public IScheduleInfoManager Create(GrainReference grainReference)
        {
            return new ScheduleInfoManager(this.logger, grainReference);
        }
    }
}