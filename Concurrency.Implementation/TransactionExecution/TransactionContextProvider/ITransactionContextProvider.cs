using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution.TransactionContextProvider
{
    public interface ITransactionContextProvider
    {
        Task<TransactionContext> GetDeterministicContext(List<GrainAccessInfo> grainAccessInfos);
    }
}