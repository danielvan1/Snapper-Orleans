namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public interface ITransactionSchedulerFactory
    {
        ITransactionScheduler Create();
    }
}