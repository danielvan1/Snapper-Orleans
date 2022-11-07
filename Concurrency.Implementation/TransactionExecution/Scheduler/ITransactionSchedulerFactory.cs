using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public interface ITransactionSchedulerFactory
    {
        ITransactionScheduler Create(GrainReference grainReference);
    }
}