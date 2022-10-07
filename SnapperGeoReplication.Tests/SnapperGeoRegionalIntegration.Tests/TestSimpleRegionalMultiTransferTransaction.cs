using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Localization;
using SmallBank.Interfaces;
using SnapperGeoReplication.Tests.ClusterSetup;
using Utilities;
using Xunit;

namespace SnapperGeoRegionalIntegration.Tests
{
    [Collection(SimpleRegionalClusterCollection.Name)]
    public class SimpleBankRegionalIntegrationTest : ClusterTestBase<RegionalIntegrationTestConfiguration>
    {
        public SimpleBankRegionalIntegrationTest(ClusterFixture<RegionalIntegrationTestConfiguration> clusterFixture) : base(clusterFixture)
        {
        }

        [Fact]
        public async void TestSimpleRegionalMultiTransferTransaction()
        {
            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

            int actorId0 = 0;
            int actorId1 = 1;
            var deployedRegion = "EU";
            var homeRegion = "EU";
            var server0 = "0";
            var server1 = "1";
            var regionAndServer0 = $"{deployedRegion}-{homeRegion}-{server0}";
            var regionAndServer1 = $"{deployedRegion}-{homeRegion}-{server1}";


            var actorAccessInfo0 = new List<GrainAccessInfo>()
            {
                new GrainAccessInfo()
                {
                    Id = actorId0,
                    Region = regionAndServer0,
                    GrainClassName = snapperTransactionalAccountGrainTypeName
                },
            };

            var actorAccessInfo1 = new List<GrainAccessInfo>()
            {
                new GrainAccessInfo()
                {
                    Id = actorId1,
                    Region = regionAndServer1,
                    GrainClassName = snapperTransactionalAccountGrainTypeName
                }
            };

            var grainClassName = new List<string>();
            grainClassName.Add(snapperTransactionalAccountGrainTypeName);

            var actor0 = this.Cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer0);
            var accountId = actorId1;

            var actor1 = this.Cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer1);

            var grainClassNamesForMultiTransfer = new List<string>();                                             // grainID, grainClassName
            grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);
            grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);

            var actorAccessInfoForMultiTransfer = new List<GrainAccessInfo>()
            {
                new GrainAccessInfo()
                {
                    Id = actorId0,
                    Region = regionAndServer0,
                    GrainClassName = snapperTransactionalAccountGrainTypeName
                },
                new GrainAccessInfo()
                {
                    Id = actorId1,
                    Region = regionAndServer1,
                    GrainClassName = snapperTransactionalAccountGrainTypeName
                },
            };

            var amountToDeposit = 50;

            try
            {
                await actor0.StartTransaction("Init", new Tuple<int, string>(actorId0, regionAndServer0), actorAccessInfo0);
                await actor1.StartTransaction("Init", new Tuple<int, string>(actorId1, regionAndServer1), actorAccessInfo1);
                var multiTransferInput = new Tuple<int, List<Tuple<int, string>>>(
                    amountToDeposit,
                    new List<Tuple<int, string>>() { new Tuple<int, string>(actorId1, regionAndServer1)
                });
                await actor0.StartTransaction("MultiTransfer", multiTransferInput, actorAccessInfoForMultiTransfer);
                var PACTBalanceActor0 = await actor0.StartTransaction("Balance", null, actorAccessInfo0);
                Xunit.Assert.Equal(9950, Convert.ToInt32(PACTBalanceActor0.resultObj));
                var PACTBalanceActor1 = await actor1.StartTransaction("Balance", null, actorAccessInfo1);
                Xunit.Assert.Equal(10050, Convert.ToInt32(PACTBalanceActor1.resultObj));
            }
            catch (Exception e)
            {
                Console.WriteLine("hi");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                // TODO: Find a nicer way to just fail the test when it catches an exception
                Xunit.Assert.True(false);
            }
        }

        [Fact]
        public async void TestAlotOfBigRegionalMultiTransferTransactions()
        {
            int numberOfAccountsInEachServer = 10;
            int theOneAccountThatSendsTheMoney = 1;
            int startAccountId0 = 0;
            int startAccountId1 = numberOfAccountsInEachServer;

            Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
            string snapperTransactionalAccountGrainTypeName = "SmallBank.Grains.SnapperTransactionalAccountGrain";
            List<GrainAccessInfo> accountIdsServer0 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId0, "EU", "EU", 0, snapperTransactionalAccountGrainTypeName);
            List<GrainAccessInfo> accountIdsServer1 = TestDataGenerator.GetAccountsFromRegion(numberOfAccountsInEachServer, startAccountId1, "EU", "EU", 1, snapperTransactionalAccountGrainTypeName);
            List<Tuple<int, string>> input0 = TestDataGenerator.GetAccountsFromRegion(accountIdsServer0);
            List<Tuple<int, string>> input1 = TestDataGenerator.GetAccountsFromRegion(accountIdsServer1);
            List<GrainAccessInfo> accountIds = accountIdsServer0.Concat(accountIdsServer1).ToList();

            Console.WriteLine($"AccountIdsServer0: {string.Join(", ", accountIdsServer0)}");
            Console.WriteLine($"AccountIdsServer1: {string.Join(", ", accountIdsServer1)}");
            Console.WriteLine($"AccountIds: {string.Join(", ", accountIds)}");

            var initTasks = new List<Task>();

            Console.WriteLine("Starting with inits");

            foreach (var accountId in accountIds)
            {
                var id = accountId.Id;
                var regionAndServer = accountId.Region;
                var actor = this.Cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);
                var initTask = actor.StartTransaction("Init", accountId, new List<GrainAccessInfo>() { accountId });
                initTasks.Add(initTask);
            }

            await Task.WhenAll(initTasks);
            Console.WriteLine("Starting with multi transfers");

            var multiTransferTasks = new List<Task>();
            var oneDollar = 1;

            foreach (var accountId in accountIdsServer0)
            {
                var multiTransferInput = new Tuple<int, List<Tuple<int, string>>>(oneDollar, input0);
                var id = accountId.Id;
                var regionAndServer = accountId.Region;
                var actor = this.Cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);
                var accounts = new List<GrainAccessInfo>(accountIdsServer0) { accountId };
                Console.WriteLine($"Accounts: {string.Join(", ", accounts)}");
                var multiTransfertask = actor.StartTransaction("MultiTransfer", multiTransferInput, accounts);
                multiTransferTasks.Add(multiTransfertask);
            }
            await Task.WhenAll(multiTransferTasks);

            Console.WriteLine("Starting with balances");

            var balanceTasks = new List<Task<TransactionResult>>();
            foreach (var accountId in accountIds)
            {
                var id = accountId.Id;
                var regionAndServer = accountId.Region;
                var actor = this.Cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(id, regionAndServer);

                Task<TransactionResult> balanceTask = actor.StartTransaction("Balance", null, new List<GrainAccessInfo>() { accountId });
                balanceTasks.Add(balanceTask);
            }

            var results = await Task.WhenAll(balanceTasks);

            Console.WriteLine("Started with checking all balances");
            var initialBalance = 10000;
            for (int i = 0; i < results.Length; i++)
            {
                var result = results[0];
                if (i < numberOfAccountsInEachServer)
                {
                    Xunit.Assert.Equal(initialBalance - numberOfAccountsInEachServer * oneDollar, Convert.ToInt32(result.resultObj));
                }
                else
                {
                    Xunit.Assert.Equal(initialBalance + numberOfAccountsInEachServer * oneDollar, Convert.ToInt32(result.resultObj));
                }
            }
        }

        public Task DisposeAsync()
        {
            this.Cluster.StopAllSilos();

            return Task.CompletedTask;
        }
    }

}