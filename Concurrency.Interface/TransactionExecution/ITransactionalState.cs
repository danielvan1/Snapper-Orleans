namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionalState<TState>
    {
        // PACT
        TState DetOp();
    }
}