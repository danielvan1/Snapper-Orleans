using System.Threading.Tasks;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Performance
{
    public interface IPerformanceGrain : IGrainWithIntegerCompoundKey
    {
        Task<double> GetAverageExecutionTime(string functionName);
        Task<double> GetAverageLatencyTime();
        Task AddTransactionResult(TransactionResult transactionResult);
    }
}