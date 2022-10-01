namespace Concurrency.Implementation.Coordinator
{
    public record TransactionId
    {
        public long Bid{ get; init; }
        public long Tid{ get; init; }
    }
}