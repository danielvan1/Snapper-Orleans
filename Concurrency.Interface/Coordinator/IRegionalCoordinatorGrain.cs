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
        Task PassToken(BasicToken token);

        Task SpawnGlobalCoordGrain(IRegionalCoordinatorGrain neighbor);

        Task<Tuple<TransactionRegistInfo, Dictionary<int, int>>> NewTransaction(List<int> siloList);
    }
}