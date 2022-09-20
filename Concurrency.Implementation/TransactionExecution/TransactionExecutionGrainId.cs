namespace Concurrency.Implementation.TransactionExecution
{
    public record TransactionExecutionGrainId
    {
        public int IntId { get; init; }

        public string StringId { get; init; }
    }
}