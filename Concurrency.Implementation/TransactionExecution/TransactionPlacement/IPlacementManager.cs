using System.Collections.Generic;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionExecution.TransactionPlacement
{
    public interface IPlacementManager
    {
        TransactionType GetTransactionType(List<GrainAccessInfo> grainAccessInfos);
    }
}