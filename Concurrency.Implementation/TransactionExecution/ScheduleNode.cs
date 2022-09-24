using System.Threading.Tasks;

namespace Concurrency.Implementation.TransactionExecution
{
    public record ScheduleNode
    {
        public long Id { get; init; }

        public bool IsDet { get; init; }

        public TaskCompletionSource<bool> NextNodeCanExecute { get; init; }

        public ScheduleNode Previous { get; set; }

        public ScheduleNode Next { get; set; }

        public ScheduleNode(long id, bool isDet)
        {
            this.Id = id;
            this.IsDet = isDet;
            this.NextNodeCanExecute = new TaskCompletionSource<bool>();
        }
    }
}