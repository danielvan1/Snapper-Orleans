using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Interface.Coordinator
{
    public interface IGlobalCoordGrain : IGrainWithIntegerCompoundKey
    {
        Task SpawnGlobalCoordGrain(IGlobalCoordGrain neighbor);

        Task PassToken(BasicToken token);

        Task<TransactionRegistInfo> NewTransaction();

        Task<Tuple<TransactionRegistInfo, Dictionary<int, int>>> NewTransaction(List<int> siloList);

        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);

        Task CheckGC();
    }
}