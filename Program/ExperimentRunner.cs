using System;
using Concurrency.Interface;
using Concurrency.Interface.Models;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;
using SnapperGeoRegionalIntegration.Tests;
using Utilities;

namespace Program
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

            var numberOfAccountsInEachServer = 10;

            var accessInfoClassNamesSingleAccess = TestDataGenerator.GetAccessInfoClassNames(1);
            var theOneAccountThatSendsTheMoney = 1;
            List<string> accessInfoClassNamesMultiTransfer = TestDataGenerator.GetAccessInfoClassNames(numberOfAccountsInEachServer+theOneAccountThatSendsTheMoney);
            int startAccountId0 = 0;
            int startAccountId1 = numberOfAccountsInEachServer;

            var accountIdsServer0 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId0, "EU", "EU", 0);
            var accountIdsServer1 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId1, "EU", "EU", 1);
            var accountIds = accountIdsServer0.Concat(accountIdsServer1).ToList();
            var initTasks = new List<Task>();

            int startBalance = 1000;

            Console.WriteLine("Starting with inits");
            foreach (var accountId in accountIds)
            {
                var id = accountId.Item1;
                var regionAndServer = accountId.Item2;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);
                var initFunctionInput = FunctionInputHelper.Create(startBalance, accountId);

                var initTask = actor.StartTransaction("Init", initFunctionInput, new List<Tuple<int, string>>() { accountId }, accessInfoClassNamesSingleAccess);
                initTasks.Add(initTask);
            }

            await Task.WhenAll(initTasks);
            // Console.WriteLine("Starting with multi transfers");


            var multiTransferTasks = new List<Task>();
            int oneDollar = 1;

            var multiTransferFunctionInput = TestDataGenerator.CreateMultiTransferFunctionInput(oneDollar, accountIdsServer1);

            foreach (var accountId in accountIdsServer0)
            {
                var id = accountId.Item1;
                var regionAndServer = accountId.Item2;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);
                var herp = accountIdsServer1.Append(accountId).ToList();

                var multiTransfertask = actor.StartTransaction("MultiTransfer", multiTransferFunctionInput, herp, accessInfoClassNamesMultiTransfer);
                multiTransferTasks.Add(multiTransfertask);
            }

            await Task.WhenAll(multiTransferTasks);

            Console.WriteLine("Starting with balances");

            var balanceTasks = new List<Task<TransactionResult>>();
            foreach (var accountId in accountIds)
            {
                var id = accountId.Item1;
                var regionAndServer = accountId.Item2;
                var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);

                Task<TransactionResult> balanceTask = actor.StartTransaction("Balance", null, new List<Tuple<int, string>>() { accountId }, accessInfoClassNamesSingleAccess);
                balanceTasks.Add(balanceTask);
            }

            var results = await Task.WhenAll(balanceTasks);

            Console.WriteLine("Started with checking all balances");
            for(int i = 0; i < results.Length; i++)
            {
                var result = results[i];
                if (i < numberOfAccountsInEachServer)
                {
                    Console.WriteLine($"result: {result.resultObj} -- expected: {startBalance - numberOfAccountsInEachServer * oneDollar}");
                }
                else
                {
                    Console.WriteLine($"result: {result.resultObj} -- expected: {startBalance + numberOfAccountsInEachServer * oneDollar}");
                }
            }
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