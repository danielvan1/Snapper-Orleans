using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Models;
using System;

namespace Concurrency.Interface.Coordinator
{
    public interface ILocalCoordinatorGrain : IGrainWithIntegerCompoundKey
    {

        Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos);

        Task<TransactionRegisterInfo> NewRegionalTransaction(long globalBid, long globalTid, List<GrainAccessInfo> grainAccessInfos);

        Task PassToken(LocalToken token);

        Task BatchCompletionAcknowledgement(long bid);

        Task WaitForBatchToCommit(long bid);

        Task RegionalBatchCommitAcknowledgement(long globalBid);

        // for global transactions (hierarchical architecture)
        Task ReceiveBatchSchedule(SubBatch batch);

        Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor);
    }
}