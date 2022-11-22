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
        Task<double> GetAveragePrepareTime(string functionName);
        Task<double> GetAverageCommitTime(string functionName);

        Task<double> GetAveragePrepareTimeReplica(string functionName, string region);
        Task<double> GetAverageCommitTimeReplica(string functionName, string region);
        Task<double> GetAverageExecutionTimeReplica(string functionName, string region);
        Task<double> GetAverageLatencyTime(string functionName, string region);
        Task<int> GetNumberOfTransactionResultsReplica(string functionName, string region);


        Task<int> NumberOfTransactions(string startFunctionName);
        Task<List<TransactionResult>> GetTransactionResults(string functionName, bool replicas);

        Task CleanUp();
    }
}