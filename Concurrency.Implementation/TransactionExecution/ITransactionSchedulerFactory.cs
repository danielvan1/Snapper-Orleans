namespace Concurrency.Implementation.TransactionExecution
{
    public interface ITransactionSchedulerFactory
    {
        ITransactionScheduler Create();
    }
}