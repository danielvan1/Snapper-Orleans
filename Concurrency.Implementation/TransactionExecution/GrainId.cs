namespace Concurrency.Implementation.TransactionExecution
{
    public record GrainId
    {
        public int IntId { get; init; }

        public string SiloId { get; init; }

        public string GrainClassName { get; init; }

        public override string ToString()
        {
            return $"{this.IntId}-{this.SiloId}";
        }
    }
}