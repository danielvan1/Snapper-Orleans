using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;

namespace Concurrency.Implementation.Coordinator.Replica
{
    public interface ILocalReplicaCoordinator : IGrainWithIntegerCompoundKey
    {

        Task ReceiveLocalSchedule(long bid, long previousBid, Dictionary<GrainAccessInfo, LocalSubBatch> schedule);

        /// <summary>
        /// Sent from the TransactionExecutionGrain that the current batch is done locally. Then check if all
        /// </summary>
        /// <param name="bid"></param>
        /// <returns></returns>
        Task CommitAcknowledgement(long bid);

        Task RegionalBatchCommitAcknowledgement(long regionalBid);
    }
}