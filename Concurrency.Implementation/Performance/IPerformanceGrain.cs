using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Performance
{
    public interface IPerformanceGrain : IGrainWithIntegerCompoundKey
    {
        Task AddTransactionResult(TransactionResult transactionResult);

        Task<double> GetAverageExecutionTime(string functionName);
        Task<double> GetAverageLatencyTime(string functionName);

        Task<int> NumberOfTransactions(string startFunctionName);

        Task<List<TransactionResult>> GetTransactionResults(string functionName, bool replicas);

        Task CleanUp();
    }
}