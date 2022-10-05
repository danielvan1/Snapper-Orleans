using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution.TransactionExecution
{
    public interface IDeterministicTransactionExecutor
    {
        Task ReceiveBatchSchedule(LocalSubBatch batch);

        Task FinishExecuteDeterministicTransaction(TransactionContext context);

        TState GetState<TState>(long tid, AccessMode mode, ITransactionalState<TState> state);

        Task WaitForTurn(TransactionContext context);

        Task AckBatchCommit(long bid);
        Task WaitForBatchToCommit(long bid);

        Task GarbageCollection(long bid);
        Task CleanUp(long tid);
    }
}