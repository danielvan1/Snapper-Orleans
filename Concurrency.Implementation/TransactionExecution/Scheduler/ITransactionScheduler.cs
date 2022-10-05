using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public interface ITransactionScheduler
    {
        void CompleteDeterministicBatch(long bid);

        void GarbageCollection(long bid);

        long IsBatchComplete(long bid, long tid);

        void RegisterBatch(SubBatch batch, long regionalBid, long highestCommittedBid);

        Task WaitForTurn(long bid, long tid);
    }
}