using System;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record TransactionInfo
    {
        public Tuple<int, string> DestinationGrain;

        public int Value;

        public override string ToString()
        {
            return $"(DesitinationGrain = {this.DestinationGrain}, Value = {this.Value})";
        }
    }
}