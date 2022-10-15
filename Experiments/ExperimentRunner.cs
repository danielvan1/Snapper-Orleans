using System;
using Concurrency.Interface;
using Concurrency.Interface.Models;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;
using SnapperGeoRegionalIntegration.Tests;
using Utilities;

namespace Experiments
{
    public class ExperimentRunner
    {
        public async Task ManyMultiTransferTransactions()
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

            var numberOfAccountsInEachServer = 70;
            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            // string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();
            string snapperTransactionalAccountGrainTypeName = "SmallBank.Grains.SnapperTransactionalAccountGrain";
            int startAccountId0 = 0;
            int startAccountId1 = numberOfAccountsInEachServer;
            var accountIdsServer0 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId0, "EU", "EU", 0, snapperTransactionalAccountGrainTypeName);
            var accountIdsServer1 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId1, "EU", "EU", 1, snapperTransactionalAccountGrainTypeName);

            var input1 = TestDataGenerator.GetAccountsFromRegion(accountIdsServer1);
            var accountIds = accountIdsServer0.Concat(accountIdsServer1).ToList();
            var initTasks = new List<Task>();

            Console.WriteLine("Starting with inits");
            foreach (var accountId in accountIds)
            {
                var id = accountId.Id;
                var regionAndServer = accountId.Region;
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
                Console.WriteLine($"accountId: {accountId}");
                var id = accountId.Id;
                var regionAndServer = accountId.Region;
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
                var id = accountId.Id;
                var regionAndServer = accountId.Region;
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
                    Console.WriteLine($"result: {result.resultObj} -- expected: {initialBalance - numberOfAccountsInEachServer * oneDollar}");
                }
                else
                {
                    Console.WriteLine($"result: {result.resultObj} -- expected: {initialBalance + numberOfAccountsInEachServer * oneDollar}");
                }
            }

            string USEU1 = "US-EU-1";
            string USEU0 = "US-EU-0";

            var replicaActorUSEU0 = client.GetGrain<ISnapperTransactionalAccountGrain>(0, USEU0);
            TransactionResult balanceTaskReplica = await replicaActorUSEU0.StartTransaction("Balance", null, new List<GrainAccessInfo>() { new GrainAccessInfo(){Id = 0, Region = USEU0, GrainClassName = snapperTransactionalAccountGrainTypeName } });
            Console.WriteLine($"Replica ::: result: {balanceTaskReplica.resultObj} -- expected {initialBalance - numberOfAccountsInEachServer * oneDollar}");

            var replicaActorUSEU1 = client.GetGrain<ISnapperTransactionalAccountGrain>(70, USEU1);
            TransactionResult balanceTaskReplica1 = await replicaActorUSEU1.StartTransaction("Balance", null, new List<GrainAccessInfo>() { new GrainAccessInfo(){Id = 70, Region = USEU1, GrainClassName = snapperTransactionalAccountGrainTypeName } });
            Console.WriteLine($"Replica ::: result: {balanceTaskReplica1.resultObj} -- expected {initialBalance + numberOfAccountsInEachServer * oneDollar}");

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
    }
}