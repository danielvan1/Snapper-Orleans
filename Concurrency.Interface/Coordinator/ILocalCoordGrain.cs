﻿using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Interface.Coordinator
{
    public interface ILocalCoordinatorGrain : IGrainWithIntegerCompoundKey
    {
        Task SpawnLocalCoordGrain(ILocalCoordinatorGrain neighbor);

        Task<TransactionRegistInfo> NewTransaction(List<int> grainAccessInfo, List<string> grainClassName);

        Task<TransactionRegistInfo> NewTransaction();

        Task PassToken(LocalToken token);

        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);

        Task AckGlobalBatchCommit(long globalBid);

        // for global transactions (hierarchical architecture)
        Task<TransactionRegistInfo> NewGlobalTransaction(long globalBid, long globalTid, List<int> grainAccessInfo, List<string> grainClassName);
        Task ReceiveBatchSchedule(SubBatch batch);
    }
}