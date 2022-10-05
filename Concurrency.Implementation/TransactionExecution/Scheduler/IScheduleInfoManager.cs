using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public interface IScheduleInfoManager
    {
        void CompleteDeterministicBatch(long bid);

        void GarbageCollection(long bid);

        ScheduleNode GetDependingNode(long bid);

        void InsertDeterministicBatch(SubBatch subBatch, long regionalBid, long highestCommittedBid);
    }
}