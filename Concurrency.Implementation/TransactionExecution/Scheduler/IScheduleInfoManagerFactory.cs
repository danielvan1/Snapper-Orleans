using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public interface IScheduleInfoManagerFactory
    {
        IScheduleInfoManager Create(GrainReference grainReference);
    }
}