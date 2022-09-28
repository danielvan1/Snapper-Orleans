using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Interface.Coordinator
{
    public interface IRegionalCoordinatorGrain : IGrainWithIntegerCompoundKey
    {
        Task PassToken(RegionalToken token);

        Task SpawnGlobalCoordGrain(IRegionalCoordinatorGrain neighbor);

        Task<Tuple<TransactionRegisterInfo, Dictionary<string, Tuple<int, string>>>> NewRegionalTransaction(List<string> siloList);

        Task AckBatchCompletion(long bid);

        Task WaitBatchCommit(long bid);
    }
}