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
        private List<TransactionResult> transactionResultsReplicas;

        public override Task OnActivateAsync()
        {
            this.functionNamesToTransactionResults = new Dictionary<string, List<TransactionResult>>();
            this.transactionResultsReplicas = new List<TransactionResult>();

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
                this.transactionResultsReplicas.Add(transactionResult);
            }

            return Task.CompletedTask;
        }

        public Task<double> GetAverageExecutionTime(string functionName)
        {
            return Task.FromResult(this.functionNamesToTransactionResults[functionName].Select(r => r.ExecuteTime)
                                                                                       .Average());
        }

        public Task<double> GetAverageLatencyTime()
        {
            return Task.FromResult(this.transactionResultsReplicas.Select(r => r.Latency)
                                                                  .Average());
        }
    }
}