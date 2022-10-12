using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation;
using Concurrency.Interface;
using Concurrency.Interface.Models;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;
using SnapperGeoRegionalIntegration.Tests;
using Utilities;

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

var numberOfAccountsInEachServer = 40;
Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
// string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();
string snapperTransactionalAccountGrainTypeName = "SmallBank.Grains.SnapperTransactionalAccountGrain";
var accessInfoClassNamesSingleAccess = TestDataGenerator.GetAccessInfoClassNames(1);
var theOneAccountThatSendsTheMoney = 1;
var accessInfoClassNamesMultiTransfer = TestDataGenerator.GetAccessInfoClassNames(numberOfAccountsInEachServer+theOneAccountThatSendsTheMoney);
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

    await Task.Delay(100);
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