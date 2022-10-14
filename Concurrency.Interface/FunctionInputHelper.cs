using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Interface
{
    public static class FunctionInputHelper
    {

        public static FunctionInput Create(int value, GrainAccessInfo grainAccessInfo)
        {
            return new FunctionInput()
            {
                DestinationGrains = new List<TransactionInfo>()
                {
                    new TransactionInfo()
                    {
                        DestinationGrain = new Tuple<int, string>(grainAccessInfo.Id, grainAccessInfo.Region),
                        Value = value
                    }
                }
            };
        }
    }
}