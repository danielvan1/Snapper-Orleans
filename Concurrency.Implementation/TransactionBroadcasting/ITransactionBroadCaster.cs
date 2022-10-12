using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public interface ITransactionBroadCaster
    {
        Task StartTransactionInAllOtherRegions(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfos, GrainId startGrain);
    }
}