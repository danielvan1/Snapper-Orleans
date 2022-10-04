namespace Concurrency.Interface.Models
{
    public record TransactionId
    {
        public long Bid{ get; init; }

        public long Tid{ get; init; }
    }
}