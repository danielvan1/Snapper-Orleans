using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Interface.Coordinator
{
    public interface IGlobalCoordinatorGrain : IGrainWithIntegerCompoundKey
    {
        Task SpawnGlobalCoordGrain(IGlobalCoordinatorGrain neighbor);

        Task PassToken(BasicToken token);

        Task<TransactionRegisterInfo> NewTransaction();

        Task<Tuple<TransactionRegisterInfo, Dictionary<string, int>>> NewTransaction(List<Tuple<int, string>> siloList);

        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);
    }
}