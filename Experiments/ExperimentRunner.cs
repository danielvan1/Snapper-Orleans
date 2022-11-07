using System;
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
                startId += grainsPerSilo;
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
                    // await actor.StartTransaction("MultiTransfer", functionInput, herp);
                    // multiTransferTasks.Add(actor.StartTransaction("MultiTransfer", functionInput, grainAccessInfos));
                }

            }

            await Task.WhenAll(multiTransferTasks);
            // var results = await Task.WhenAll(multiTransferTasks);

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
            Console.WriteLine($"Herp {results.Length}");

            for(int i = 0; i < results.Length; i++)
            {
                var result = results[i];
                if (i < numberOfAccountsInEachServer)
                {
                    Console.WriteLine($"result: {result.resultObj} -- expected: {initialBalance - numberOfAccountsInEachServer * oneDollar}");
                }
                else
                {
                    Console.WriteLine($"result: {result.resultObj} -- expected: {initialBalance + numberOfAccountsInEachServer * oneDollar}");
                }
            }

            string USEU1 = $"US-{region}-1";
            string USEU0 = $"US-{region}-0";

            await Task.Delay(1000);
            var replicaGrain0 = client.GetGrain<ISnapperTransactionalAccountGrain>(0, USEU0);
            var replicaGrain1 = client.GetGrain<ISnapperTransactionalAccountGrain>(multitransfers, USEU1);
            var bankaccount0 = await replicaGrain0.GetState();
            var bankaccount1 = await replicaGrain1.GetState();
            Console.WriteLine($"Replica0 account balance: {bankaccount0.balance}");
            Console.WriteLine($"Replica1 account balance: {bankaccount1.balance}");

            await client.Close();
        }

        public async Task SimpleBank()
        {
            var client = new ClientBuilder()
            .UseLocalhostClustering()
            .Configure<ClusterOptions>(options =>
            {
                options.ClusterId = "Snapper";
                options.ServiceId = "Snapper";
            })
            .Build();

            await client.Connect();

            // Going to perform 2 init transactions on two accounts in the same region,
            // and then transfer 50$ from account id 0 to account id 1. They both
            // get initialized to 100$(hardcoded inside of Init)

            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

            int actorId0 = 0;
            int actorId1 = 1;
            var regionAndServer = "EU-EU-0";

            var actorAccessInfo0 = new List<Tuple<int, string>>()
            {
                new Tuple<int, string>(actorId0, regionAndServer),
            };

            var actorAccessInfo1 = new List<Tuple<int, string>>()
            {
                new Tuple<int, string>(actorId1, regionAndServer),
            };

            var grainClassName = new List<string>();
            grainClassName.Add(snapperTransactionalAccountGrainTypeName);

            var actor0 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer);
            var accountId = actorId1;

            var actor1 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer);

            var grainClassNamesForMultiTransfer = new List<string>();                                             // grainID, grainClassName
            grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);
            grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);

            var actorAccessInfoForMultiTransfer = new List<Tuple<int, string>>()
            {
                new Tuple<int, string>(actorId0, regionAndServer),
                new Tuple<int, string>(actorId1, regionAndServer),
            };


            var amountToDeposit = 50;
            var multiTransferInput = new Tuple<int, List<Tuple<int, string>>>(
                amountToDeposit,
                new List<Tuple<int, string>>() { new Tuple<int, string>(actorId1, regionAndServer)
            });

            try
            {
                Console.WriteLine("Starting init txs(both accounts start with 100$)");
                // await actor0.StartTransaction("Init", new Tuple<int, string>(actorId0, regionAndServer), actorAccessInfo0, grainClassName);
                // await actor1.StartTransaction("Init", new Tuple<int, string>(actorId1, regionAndServer), actorAccessInfo1, grainClassName);

                // Console.WriteLine("Starting deposit txs");

                // await actor0.StartTransaction("MultiTransfer", multiTransferInput, actorAccessInfoForMultiTransfer, grainClassNamesForMultiTransfer);

                // Console.WriteLine("Starting balance txs");

                // var PACT_balance3 = await actor0.StartTransaction("Balance", null, actorAccessInfo0, grainClassName);
                // Console.WriteLine($"The PACT balance in actor {actorId0} after giving money: Expected: 50, Actual:{PACT_balance3.resultObj}");

                // var PACT_balance4 = await actor1.StartTransaction("Balance", null, actorAccessInfo1, grainClassName);
                // Console.WriteLine($"The PACT balance in actor {actorId1} after receiving money: Expected: 150, Actual:{PACT_balance4.resultObj}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }

            Console.WriteLine("Ended deterministic tx");

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