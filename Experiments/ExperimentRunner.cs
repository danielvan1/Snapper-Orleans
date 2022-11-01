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
            var accountIdsServer0 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId0, region, region, 0, snapperTransactionalAccountGrainTypeName);
            var accountIdsServer1 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId1, region, region, 1, snapperTransactionalAccountGrainTypeName);

            var input1 = TestDataGenerator.GetAccountsFromRegion(accountIdsServer1);
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
    }
}