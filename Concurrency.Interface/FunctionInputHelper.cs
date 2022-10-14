using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Interface
{
    public static class FunctionInputHelper
    {

        public static FunctionInput Create(int value, Tuple<int, string> destinationGrain)
        {
            return new FunctionInput()
            {
                DestinationGrains = new List<TransactionInfo>()
                {
                    new TransactionInfo()
                    {
                        DestinationGrain = destinationGrain,
                        Value = value
                    }
                }
            };
        }
    }
}