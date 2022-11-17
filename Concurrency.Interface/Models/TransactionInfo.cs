using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record TransactionInfo
    {
        public Tuple<int, string> DestinationGrain;

        public float Value;

        public float SecondValue;

        public override string ToString()
        {
            return $"(DesitinationGrain = {this.DestinationGrain}, Value = {this.Value})";
        }
    }
}