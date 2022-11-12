using System;
using Concurrency.Implementation.Performance;
using Concurrency.Interface;
using Concurrency.Interface.Models;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;
using SnapperGeoRegionalIntegration.Tests;
using ThirdParty.BouncyCastle.Asn1;
using Utilities;

namespace Experiments
{
    public class ExperimentRunner
    {
       public async Task StressRun(IClusterClient client, string region, int silos, int grainsPerSilo)
        {
            await client.Connect();

            List<GrainAccessInfo>[] accountIds = new List<GrainAccessInfo>[silos];
            int startId = 0;

            for (int siloIndex = 0; siloIndex < silos; siloIndex++)
            {
                accountIds[siloIndex] = (TestDataGenerator.CreateAccountIds(grainsPerSilo, startId, region, region, siloIndex, "SmallBank.Grains.SnapperTransactionalAccountGrain"));
                // startId += grainsPerSilo * silos + 1;
            }

            int initialBalance = 1000;

            foreach(var grainAccessInfoList in accountIds)
            {
                var initResults = await this.InitAccountsAsync(grainAccessInfoList, client, initialBalance);
            }

            var multiTransferTasks = new List<Task<TransactionResult>>();

            for (int siloIndex = 0; siloIndex < accountIds.Length; siloIndex++)
            {
                List<GrainAccessInfo> grainAccessInfos = new List<GrainAccessInfo>();
                var currentGrains = accountIds[siloIndex];

                for (int next = 0; next < accountIds.Length; next++)
                {
                    if(siloIndex == next) continue;

                    grainAccessInfos.AddRange(accountIds[next]);
                }

                FunctionInput functionInput = TestDataGenerator.CreateFunctionInput(grainAccessInfos);

                for (int i = 0; i < grainsPerSilo; i++)
                {
                    var grain = currentGrains[i];
                    var herp = grainAccessInfos.Append(grain).ToList();

                    var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(grain.Id, grain.SiloId);
                    multiTransferTasks.Add(actor.StartTransaction("MultiTransfer", functionInput, herp));
                }

            }

            await Task.WhenAll(multiTransferTasks);

            var balanceTasks = new List<TransactionResult>();

            foreach(var grainAccessInfoList in accountIds)
            {
                var balanceResult = await this.GetAccountBalancesAsync(grainAccessInfoList, client);
                Console.WriteLine($"BalanceResults: [{string.Join(", ", balanceResult.Select(r => r.Result))}]");
            }

            await Task.Delay(20000);

            var performanceGrain = client.GetGrain<IPerformanceGrain>(0, "US");

            Console.WriteLine($"AverageExecutionTime MultiTransfer: {await performanceGrain.GetAverageExecutionTime("MultiTransfer")}");
            Console.WriteLine($"AverageExecutionTime Balance:: {await performanceGrain.GetAverageExecutionTime("Balance")}");
            Console.WriteLine($"AverageLatency MultiTransfer: {await performanceGrain.GetAverageLatencyTime("MultiTransfer")}");
            Console.WriteLine($"AverageLatency Balance: {await performanceGrain.GetAverageLatencyTime("Init")}");
            Console.WriteLine($"Replica MultiTransfer Results: [{string.Join(", ", (await performanceGrain.GetTransactionResults("MultiTransfer", true)).Select(r => r.Result))}]");
            Console.WriteLine($"Replica Balance Results: [{string.Join(", ", (await performanceGrain.GetTransactionResults("Balance", true)).Select(r => r.Result))}]");

            await client.Close();
        }

        public async Task ManyMultiTransferTransactions(IClusterClient client, string region, int multitransfers)
        {
            await client.Connect();

            // Going to perform 2 init transactions on two accounts in the same region,
            // and then transfer 50$ from account id 0 to account id 1. They both

            var numberOfAccountsInEachServer = multitransfers;
            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            // string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();
            string snapperTransactionalAccountGrainTypeName = "SmallBank.Grains.SnapperTransactionalAccountGrain";
            int startAccountId0 = 0;
            int startAccountId1 = numberOfAccountsInEachServer;
            var accountIdsServer0 = TestDataGenerator.CreateAccountIds(numberOfAccountsInEachServer, startAccountId0, region, region, 0, snapperTransactionalAccountGrainTypeName);
            var accountIdsServer1 = TestDataGenerator.CreateAccountIds(numberOfAccountsInEachServer, startAccountId1, region, region, 1, snapperTransactionalAccountGrainTypeName);

            var input1 = TestDataGenerator.CreateFunctionInput(accountIdsServer1);
            var accountIds = accountIdsServer0.Concat(accountIdsServer1).ToList();
            var initTasks = new List<Task>();

            Console.WriteLine("Starting with inits");
            foreach (var accountId in accountIds)
            {
                var id = accountId.Id;
                var regionAndServer = accountId.SiloId;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);
                var initTask = actor.StartTransaction("Init", FunctionInputHelper.Create(1000, new Tuple<int, string>(id, regionAndServer)), new List<GrainAccessInfo>() { accountId });

                initTasks.Add(initTask);
            }

            await Task.WhenAll(initTasks);
            Console.WriteLine("Starting with multi transfers");

            var multiTransferTasks = new List<Task>();
            var oneDollar = 1;
            foreach (var accountId in accountIdsServer0)
            {
                var id = accountId.Id;
                var regionAndServer = accountId.SiloId;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);

                var herp = accountIdsServer1.Append(accountId).ToList();

                // await Task.Delay(1000);
                var multiTransfertask = actor.StartTransaction("MultiTransfer", input1, herp);
                multiTransferTasks.Add(multiTransfertask);
            }
            await Task.WhenAll(multiTransferTasks);

            Console.WriteLine("Starting with balances");

            var balanceTasks = new List<Task<TransactionResult>>();

            foreach (var accountId in accountIds)
            {
                Console.WriteLine($"accountId: {accountId}");
                var id = accountId.Id;
                var regionAndServer = accountId.SiloId;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);

                Task<TransactionResult> balanceTask = actor.StartTransaction("Balance", null, new List<GrainAccessInfo>() { accountId });

                balanceTasks.Add(balanceTask);
            }

            var results = await Task.WhenAll(balanceTasks);

            int initialBalance = 1000;

            for(int i = 0; i < results.Length; i++)
            {
                var result = results[i];
                if (i < numberOfAccountsInEachServer)
                {
                    Console.WriteLine($"result: {result.Result} -- expected: {initialBalance - numberOfAccountsInEachServer * oneDollar}");
                }
                else
                {
                    Console.WriteLine($"result: {result.Result} -- expected: {initialBalance + numberOfAccountsInEachServer * oneDollar}");
                }
            }

            string USEU1 = $"US-{region}-1";
            string USEU0 = $"US-{region}-0";

            await Task.Delay(6000);
            var replicaGrain0 = client.GetGrain<ISnapperTransactionalAccountGrain>(0, USEU0);
            var replicaGrain1 = client.GetGrain<ISnapperTransactionalAccountGrain>(multitransfers, USEU1);
            var bankaccount0 = await replicaGrain0.GetState();
            var bankaccount1 = await replicaGrain1.GetState();
            Console.WriteLine($"Replica0 account balance: {bankaccount0.balance}");
            Console.WriteLine($"Replica1 account balance: {bankaccount1.balance}");

            await client.Close();
        }

        private async Task<TransactionResult[]> InitAccountsAsync(List<GrainAccessInfo> accountIds, IClusterClient client, int initialBalance)
        {
            Console.WriteLine("Start Init accounts");

            var initTasks = new List<Task<TransactionResult>>();

            foreach (var accountId in accountIds)
            {
                var id = accountId.Id;
                var regionAndServer = accountId.SiloId;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);
                var initTask = actor.StartTransaction("Init", FunctionInputHelper.Create(initialBalance, new Tuple<int, string>(id, regionAndServer)), new List<GrainAccessInfo>() { accountId });

                initTasks.Add(initTask);
            }

            var initResults = await Task.WhenAll(initTasks);

            Console.WriteLine("End Init accounts");

            return initResults;
        }

        private async Task<TransactionResult[]> GetAccountBalancesAsync(List<GrainAccessInfo> accountIds, IClusterClient client)
        {
            Console.WriteLine("Starting to get Balances");

            var balanceTasks = new List<Task<TransactionResult>>();

            foreach (var accountId in accountIds)
            {
                Console.WriteLine($"accountId: {accountId}");
                var id = accountId.Id;
                var regionAndServer = accountId.SiloId;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);

                Task<TransactionResult> balanceTask = actor.StartTransaction("Balance", null, new List<GrainAccessInfo>() { accountId });

                balanceTasks.Add(balanceTask);
            }

            var balances = await Task.WhenAll(balanceTasks);

            Console.WriteLine("End of getting balances");

            return balances;
        }

        private void PrintPerformance(Dictionary<string, List<TransactionResult>> transactionResultsPerRegion, string method)
        {
            foreach((string region, var transactionResults) in transactionResultsPerRegion)
            {
                // Console.WriteLine(string.Join(", ", transactionResults.Select(t => $"({region}, {method} PrepareTime: {t.PrepareTime}, ExecuteTime: {t.ExecuteTime}, CommitTime: {t.CommitTime}, Latency: {t.Latency})")));
            }
        }
    }
}