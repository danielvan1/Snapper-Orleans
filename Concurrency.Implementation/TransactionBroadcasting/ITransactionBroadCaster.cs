using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Models;
using Utilities;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public interface ITransactionBroadCaster
    {
        Task StartTransactionInAllOtherRegions(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfos, GrainId startGrain, TransactionContext transactionContext, long highestCommittedBidFromMaster);

        Task BroadCastLocalSchedules(string currentLocalSiloId, long bid, long previousBid, Dictionary<GrainAccessInfo, LocalSubBatch> replicaSchedules);

        Task BroadCastRegionalSchedules(string currentRegionSiloId, long bid, long previousBid, Dictionary<string, SubBatch> replicaSchedules);
    }
}