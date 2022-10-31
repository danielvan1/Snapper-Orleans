using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Interface.Coordinator
{
    public interface ILocalCoordinatorGrain : IGrainWithIntegerCompoundKey
    {
        Task AckRegionalBatchCommit(long regionalBid);

        Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos);

        Task<TransactionRegisterInfo> NewRegionalTransaction(long globalBid, long globalTid, List<GrainAccessInfo> grainAccessInfos);

        Task PassToken(LocalToken token);

        Task BatchCompletionAcknowledgement(long bid);

        Task WaitForBatchToCommit(long bid);


        // for global transactions (hierarchical architecture)
        Task ReceiveBatchSchedule(SubBatch batch);

        Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor);
    }
}