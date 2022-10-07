using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public interface ITransactionBroadCaster
    {
        Task StartTransactionInAllOtherRegions(string firstFunction, object functionInput, List<GrainAccessInfo> grainAccessInfos);
    }
}