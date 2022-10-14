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

var numberOfAccountsInEachServer = 100;

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