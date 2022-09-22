using System.Threading.Tasks;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionalState<TState>
    {
        // PACT
        TState DetOp();
    }
}