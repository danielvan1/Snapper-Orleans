namespace Concurrency.Implementation.TransactionExecution
{
    public record GrainId
    {
        public int IntId { get; init; }

        public string StringId { get; init; }
    }
}