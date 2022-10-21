using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;

namespace Concurrency.Implementation.Coordinator.Replica
{
    public interface IRegionalReplicaCoordinator : IGrainWithIntegerCompoundKey
    {

        Task ReceiveRegionalSchedule(long regionalBid, long previousRegionalBid, Dictionary<string, SubBatch> schedule);

        /// <summary>
        /// Sent from the TransactionExecutionGrain that the current batch is done locally. Then check if all
        /// </summary>
        /// <param name="bid"></param>
        /// <returns></returns>
        Task CommitAcknowledgement(long bid);
    }
}