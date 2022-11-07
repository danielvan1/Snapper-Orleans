using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Performance
{
    public interface IPerformanceManagerGrain : IGrainWithIntegerCompoundKey
    {
        Task AddTransactionResult(TransactionResult transactionResult, string region);

        Task<Dictionary<string, List<TransactionResult>>> GetTransactionResults();
    }
}