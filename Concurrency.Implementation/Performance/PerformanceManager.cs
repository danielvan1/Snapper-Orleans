using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Performance
{
    // [RegionalSiloGrainPlacementStrategy]
    public class PerformanceManagerGrain : Grain, IPerformanceManagerGrain
    {
        private Dictionary<string, List<TransactionResult>> transactionResults;

        public override Task OnActivateAsync()
        {
            this.transactionResults = new Dictionary<string, List<TransactionResult>>();

            return Task.CompletedTask;
        }

        public Task AddTransactionResult(TransactionResult transactionResult, string region)
        {
            if(!this.transactionResults.ContainsKey(region))
            {
                this.transactionResults.Add(region, new List<TransactionResult>());
            }

            this.transactionResults[region].Add(transactionResult);

            return Task.CompletedTask;
        }

        public Task<Dictionary<string, List<TransactionResult>>> GetTransactionResults()
        {
            var copy = new Dictionary<string, List<TransactionResult>>(this.transactionResults);
            var result = Task.FromResult<Dictionary<string, List<TransactionResult>>>(copy);

            this.transactionResults.Clear();

            return result;
        }
    }
}