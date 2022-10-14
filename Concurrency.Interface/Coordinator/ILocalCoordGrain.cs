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
        Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor);

        Task<TransactionRegisterInfo> NewLocalTransaction(List<GrainAccessInfo> grainAccessInfos);

        Task PassToken(LocalToken token);

        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);

        Task AckRegionalBatchCommit(long globalBid);

        // for global transactions (hierarchical architecture)
        Task<TransactionRegisterInfo> NewRegionalTransaction(long globalBid, long globalTid, List<GrainAccessInfo> grainAccessInfo);

        Task ReceiveBatchSchedule(SubBatch batch);
    }
}