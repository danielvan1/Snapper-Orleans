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

        Task<TransactionRegisterInfo> NewTransaction(List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName);

        Task PassToken(LocalToken token);

        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);

        Task AckRegionalBatchCommit(long globalBid);

        // for global transactions (hierarchical architecture)
        Task<TransactionRegisterInfo> NewRegionalTransaction(long globalBid, long globalTid, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName);
        Task ReceiveBatchSchedule(SubBatch batch);
    }
}