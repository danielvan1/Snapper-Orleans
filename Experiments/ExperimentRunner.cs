using System.Diagnostics;
using Concurrency.Implementation.Performance;
using Concurrency.Interface;
using Concurrency.Interface.Models;
using Orleans;
using SmallBank.Interfaces;
using SnapperGeoRegionalIntegration.Tests;
using Utilities;

namespace Experiments
{
    public class ExperimentRunner
    {
        private const string US = "US";
        private const string EU = "EU";
        private const string ASIA = "AS";

        public async Task CorrectnessCheck(IClusterClient client, string region, int silos, int grainsPerSilo)
       {
            await client.Connect();
            Random random = new Random();
            List<GrainAccessInfo>[] accountIds = new List<GrainAccessInfo>[silos];
            int startId = 0;

            for (int siloIndex = 0; siloIndex < silos; siloIndex++)
            {
                accountIds[siloIndex] = (TestDataGenerator.CreateAccountIds(grainsPerSilo, startId, region, region, siloIndex, "SmallBank.Grains.SnapperTransactionalAccountGrain"));
            }

            int initialBalance = 10000;
            var initTasks = new List<Task>();

            foreach(var grainAccessInfoList in accountIds)
            {
                initTasks.Add(this.InitAccountsAsync(grainAccessInfoList, client, initialBalance));
            }

            await Task.WhenAll(initTasks);

            var multiTransferTasks = new List<Task<TransactionResult>>();
            int runs = 4;

            for (int siloIndex = 0; siloIndex < accountIds.Length; siloIndex++)
            {
                var currentGrains = accountIds[siloIndex];

                foreach(GrainAccessInfo grainAccessInfo in currentGrains)
                {
                    List<GrainAccessInfo> grainAccessInfos = new List<GrainAccessInfo>(){grainAccessInfo};

                    for (int r = 0; r < runs; r++)
                    {
                        FunctionInput functionInput = TestDataGenerator.CreateFunctionInput(grainAccessInfo, random.Next(2, 4), random.Next(1001, 2002));
                        var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(grainAccessInfo.Id, grainAccessInfo.SiloId);
                        multiTransferTasks.Add(actor.StartTransaction("Remove", functionInput, grainAccessInfos));
                    }
                }
            }

            await Task.WhenAll(multiTransferTasks);

            foreach(var grainAccessInfoList in accountIds)
            {
                var balanceResult = await this.GetAccountBalancesAsync(grainAccessInfoList, client);
                Console.WriteLine($"BalanceResults: [{string.Join(", ", balanceResult.Select(r => $"{r.GrainId} = {r.Result}"))}]");
            }

            await Task.Delay(10000);

            var performanceGrain = client.GetGrain<IPerformanceGrain>(0, "US");
            const string Init = "Init";
            const string Balance = "Balance";

            Console.WriteLine($"LocalCoordinators: {Constants.NumberOfLocalCoordinatorsPerSilo}");
            Console.WriteLine($"RegionalCoordinators: {Constants.NumberOfRegionalCoordinators}");

            Console.WriteLine($"AverageExecutionTime Balance:: {await performanceGrain.GetAverageExecutionTime(Balance)}");
            Console.WriteLine($"AverageLatency Balance: {await performanceGrain.GetAverageLatencyTime(Init, "US")}");
            Console.WriteLine($"Replica Balance Results: [{string.Join(", ", (await performanceGrain.GetTransactionResults(Balance, true)).Select(r => $"{r.GrainId} = {r.Result}"))}]");

            await client.Close();
       }

       public async Task StressRun1(IClusterClient client, string region, int silos, int grainsPerSilo, int transactionSize)
       {
            List<GrainAccessInfo>[] accountIds = new List<GrainAccessInfo>[silos];
            int startId = 0;

            for (int siloIndex = 0; siloIndex < silos; siloIndex++)
            {
                accountIds[siloIndex] = (TestDataGenerator.CreateAccountIds(grainsPerSilo, startId, region, region, siloIndex, "SmallBank.Grains.SnapperTransactionalAccountGrain"));
                // startId += grainsPerSilo * silos + 1;
            }

            int initialBalance = 10000;
            var initTasks = new List<Task>();

            Stopwatch stopwatch = Stopwatch.StartNew();

            foreach(var grainAccessInfoList in accountIds)
            {
                initTasks.Add(this.InitAccountsAsync(grainAccessInfoList, client, initialBalance));
            }

            await Task.WhenAll(initTasks);

            stopwatch.Stop();
            long initTime = stopwatch.ElapsedMilliseconds;

            stopwatch = Stopwatch.StartNew();
            var multiTransferTasks = new List<Task<TransactionResult>>();

            for (int siloIndex = 0; siloIndex < accountIds.Length; siloIndex++)
            {
                List<GrainAccessInfo> grainAccessInfos = new List<GrainAccessInfo>();
                var currentGrains = accountIds[siloIndex];

                for (int next = 0; next < accountIds.Length; next++)
                {
                    if(siloIndex == next) continue;

                    for (int i = 0; i < transactionSize; i++)
                    {
                        grainAccessInfos.Add(accountIds[next][i]);
                    }
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

            stopwatch.Stop();
            long multihomeTime = stopwatch.ElapsedMilliseconds;

            // foreach(var grainAccessInfoList in accountIds)
            // {
            //     var balanceResult = await this.GetAccountBalancesAsync(grainAccessInfoList, client);
            //     // Console.WriteLine($"BalanceResults: [{string.Join(", ", balanceResult.Select(r => r.Result))}]");
            // }

        }

       public async Task StressRun(IClusterClient client, string region, int silos, int grainsPerSilo, int transactionSize)
       {
            await client.Connect();

            List<GrainAccessInfo>[] accountIds = new List<GrainAccessInfo>[silos];
            int startId = 0;


            for (int siloIndex = 0; siloIndex < silos; siloIndex++)
            {
                accountIds[siloIndex] = (TestDataGenerator.CreateAccountIds(grainsPerSilo, startId, region, region, siloIndex, "SmallBank.Grains.SnapperTransactionalAccountGrain"));
                // startId += grainsPerSilo * silos + 1;
            }

            int initialBalance = 10000;
            var initTasks = new List<Task>();

            Stopwatch stopwatch = Stopwatch.StartNew();

            foreach(var grainAccessInfoList in accountIds)
            {
                initTasks.Add(this.InitAccountsAsync(grainAccessInfoList, client, initialBalance));
            }

            await Task.WhenAll(initTasks);

            stopwatch.Stop();
            long initTime = stopwatch.ElapsedMilliseconds;

            stopwatch = Stopwatch.StartNew();
            var multiTransferTasks = new List<Task<TransactionResult>>();

            for (int siloIndex = 0; siloIndex < accountIds.Length; siloIndex++)
            {
                List<GrainAccessInfo> grainAccessInfos = new List<GrainAccessInfo>();
                var currentGrains = accountIds[siloIndex];

                for (int next = 0; next < accountIds.Length; next++)
                {
                    if(siloIndex == next) continue;

                    for (int i = 0; i < transactionSize; i++)
                    {
                        grainAccessInfos.Add(accountIds[next][i]);
                    }
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

            stopwatch.Stop();
            long multihomeTime = stopwatch.ElapsedMilliseconds;

            var balanceTasks = new List<TransactionResult>();

            stopwatch = Stopwatch.StartNew();

            foreach(var grainAccessInfoList in accountIds)
            {
                var balanceResult = await this.GetAccountBalancesAsync(grainAccessInfoList, client);
                // Console.WriteLine($"BalanceResults: [{string.Join(", ", balanceResult.Select(r => r.Result))}]");
            }

            stopwatch.Stop();
            long balanceTime = stopwatch.ElapsedMilliseconds;

            await Task.Delay(20000);

            var performanceGrain = client.GetGrain<IPerformanceGrain>(0, "US");
            const string Init = "Init";
            const string MultiTransfer = "MultiTransfer";
            const string Balance = "Balance";

            Console.WriteLine($"LocalCoordinators: {Constants.NumberOfLocalCoordinatorsPerSilo}");
            Console.WriteLine($"RegionalCoordinators: {Constants.NumberOfRegionalCoordinators}");
            Console.WriteLine($"TransactionSize: {transactionSize}");

            int initTransactions = await performanceGrain.NumberOfTransactions(Init);
            int multiTransfers = await performanceGrain.NumberOfTransactions(MultiTransfer);
            int balanceTransactions = await performanceGrain.NumberOfTransactions(Balance);

            Console.WriteLine($"Number of init transactions: {initTransactions}");
            Console.WriteLine($"Number of multitransfer transactions: {multiTransfers}");
            Console.WriteLine($"Number of balance transactions: {balanceTransactions}");
            Console.WriteLine($"Total transactions: {initTransactions + multiTransfers + balanceTransactions}");

            Console.WriteLine($"AveragePrepareTime Init: {await performanceGrain.GetAveragePrepareTime(Init)}");
            Console.WriteLine($"AveragePrepareTime MultiTransfer: {await performanceGrain.GetAveragePrepareTime(MultiTransfer)}");
            Console.WriteLine($"AveragePrepareTime Balance:: {await performanceGrain.GetAveragePrepareTime(Balance)}");

            Console.WriteLine($"AverageExecutionTime Init: {await performanceGrain.GetAverageExecutionTime(Init)}");
            Console.WriteLine($"AverageExecutionTime MultiTransfer: {await performanceGrain.GetAverageExecutionTime(MultiTransfer)}");
            Console.WriteLine($"AverageExecutionTime Balance:: {await performanceGrain.GetAverageExecutionTime(Balance)}");

            Console.WriteLine($"AverageCommitTime Init: {await performanceGrain.GetAverageCommitTime(Init)}");
            Console.WriteLine($"AverageCommitTime MultiTransfer: {await performanceGrain.GetAverageCommitTime(MultiTransfer)}");
            Console.WriteLine($"AverageCommitTime Balance:: {await performanceGrain.GetAverageCommitTime(Balance)}");

            Console.WriteLine($"AverageExecutionTime Replica Init US: {await performanceGrain.GetAverageExecutionTimeReplica(Init, US)}");
            Console.WriteLine($"AverageExecutionTime Replica MultiTransfer US: {await performanceGrain.GetAverageExecutionTimeReplica(MultiTransfer, US)}");
            Console.WriteLine($"AverageExecutionTime Replica Balance US: {await performanceGrain.GetAverageExecutionTimeReplica(Balance, US)}");

            Console.WriteLine($"AverageLatency Init US: {await performanceGrain.GetAverageLatencyTime(Init, US)}");
            Console.WriteLine($"AverageLatency MultiTransfer US: {await performanceGrain.GetAverageLatencyTime(MultiTransfer, US)}");
            Console.WriteLine($"AverageLatency Balance US: {await performanceGrain.GetAverageLatencyTime(Balance, US)}");

            Console.WriteLine($"AverageExecutionTime Replica Init ASIA: {await performanceGrain.GetAverageExecutionTimeReplica(Init, ASIA)}");
            Console.WriteLine($"AverageExecutionTime Replica MultiTransfer ASIA: {await performanceGrain.GetAverageExecutionTimeReplica(MultiTransfer, ASIA)}");
            Console.WriteLine($"AverageExecutionTime Replica Balance ASIA: {await performanceGrain.GetAverageExecutionTimeReplica(Balance, ASIA)}");

            Console.WriteLine($"AverageLatency Init ASIA: {await performanceGrain.GetAverageLatencyTime(Init, ASIA)}");
            Console.WriteLine($"AverageLatency MultiTransfer ASIA: {await performanceGrain.GetAverageLatencyTime(MultiTransfer, ASIA)}");
            Console.WriteLine($"AverageLatency Balance ASIA: {await performanceGrain.GetAverageLatencyTime(Balance, ASIA)}");

            await performanceGrain.CleanUp();

            // Console.WriteLine($"Replica MultiTransfer Results: [{string.Join(", ", (await performanceGrain.GetTransactionResults(MultiTransfer, true)).Select(r => r.Result))}]");
            // Console.WriteLine($"Replica Balance Results: [{string.Join(", ", (await performanceGrain.GetTransactionResults(Balance, true)).Select(r => r.Result))}]");

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
            Console.WriteLine($"Replica0 account balance: {bankaccount0.Balance}");
            Console.WriteLine($"Replica1 account balance: {bankaccount1.Balance}");

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
    }
}