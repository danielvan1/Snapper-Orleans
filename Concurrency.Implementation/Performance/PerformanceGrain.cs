using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Performance
{
    [RegionalCoordinatorGrainPlacementStrategy]
    public class PerformanceGrain : Grain, IPerformanceGrain
    {
        private Dictionary<string, List<TransactionResult>> functionNamesToTransactionResults;
        private Dictionary<string, List<TransactionResult>> functionNamesTotransactionResultsReplicas;

        public override Task OnActivateAsync()
        {
            this.functionNamesToTransactionResults = new Dictionary<string, List<TransactionResult>>();
            this.functionNamesTotransactionResultsReplicas = new Dictionary<string, List<TransactionResult>>();

            return Task.CompletedTask;
        }

        public Task AddTransactionResult(TransactionResult transactionResult)
        {
            string functionName = transactionResult.FirstFunctionName;

            if (!transactionResult.IsReplica)
            {
                if (!this.functionNamesToTransactionResults.ContainsKey(functionName))
                {
                    this.functionNamesToTransactionResults.Add(functionName, new List<TransactionResult>());
                }

                this.functionNamesToTransactionResults[functionName].Add(transactionResult);
            }
            else
            {
                if (!this.functionNamesTotransactionResultsReplicas.ContainsKey(functionName))
                {
                    this.functionNamesTotransactionResultsReplicas.Add(functionName, new List<TransactionResult>());
                }

                this.functionNamesTotransactionResultsReplicas[functionName].Add(transactionResult);
            }

            return Task.CompletedTask;
        }

        public Task<int> NumberOfTransactions(string startFunctionName)
        {
            return Task.FromResult(this.functionNamesToTransactionResults[startFunctionName].Count);
        }

        public Task<double> GetAverageExecutionTime(string functionName)
        {
            return Task.FromResult(this.functionNamesToTransactionResults[functionName].Select(r => r.ExecuteTime)
                                                                                       .Average());
        }

        public Task<double> GetAverageLatencyTime(string functionName)
        {
            return Task.FromResult(this.functionNamesTotransactionResultsReplicas[functionName].Select(r => r.Latency)
                                                                                               .Average());
        }

        public Task<List<TransactionResult>> GetTransactionResults(string functionName, bool replicas)
        {
            return Task.FromResult(replicas
                                   ? this.functionNamesTotransactionResultsReplicas[functionName]
                                   : this.functionNamesToTransactionResults[functionName]);


        }

        public Task CleanUp()
        {
            this.functionNamesToTransactionResults.Clear();
            this.functionNamesTotransactionResultsReplicas.Clear();

            return Task.CompletedTask;
        }
    }
}