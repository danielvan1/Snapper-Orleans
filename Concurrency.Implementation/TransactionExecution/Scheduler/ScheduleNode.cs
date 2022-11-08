using System.Threading.Tasks;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public record ScheduleNode
    {
        public long Bid { get; init; }

        public bool IsDet { get; init; }

        public TaskCompletionSource<bool> NextNodeCanExecutePromise { get; init; }

        public ScheduleNode Previous { get; set; }

        public ScheduleNode Next { get; set; }

        public ScheduleNode(long bid, bool isDet)
        {
            this.Bid = bid;
            this.IsDet = isDet;
            this.NextNodeCanExecutePromise = new TaskCompletionSource<bool>();
        }
    }
}