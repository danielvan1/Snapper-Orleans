namespace Concurrency.Implementation.TransactionExecution
{
    public record GrainId
    {
        public int IntId { get; init; }

        public string SiloId { get; init; }

        public string GrainClassName { get; init; }
    }
}