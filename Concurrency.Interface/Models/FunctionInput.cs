using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record FunctionInput
    {
        public List<TransactionInfo> DestinationGrains {get; init;}

        public override string ToString()
        {
            return $"[{string.Join(", ", this.DestinationGrains)}]";
        }
    }
}