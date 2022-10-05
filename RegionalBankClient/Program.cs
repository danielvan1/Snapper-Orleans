using Concurrency.Interface.Configuration;
using Concurrency.Interface.Models;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;

var client = new ClientBuilder()
.UseLocalhostClustering()
.Configure<ClusterOptions>(options =>
{
    options.ClusterId = "Snapper";
    options.ServiceId = "Snapper";
})
.Configure<ClientMessagingOptions>(options =>
{
    options.ResponseTimeout = new TimeSpan(0, 2, 0);
})
.Build();

await client.Connect();

Console.WriteLine("Regional Bank Client is ready");


// Going to perform 2 init transactions on two accounts in the same region,
// and then transfer 50$ from account id 0 to account id 1. They both
// get initialized to 100$(hardcoded inside of Init)

Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

int actorId0 = 0;
int actorId1 = 4;
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
    }
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

var actor0 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer0);
var accountId = actorId1;

var actor1 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer1);


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

try {
    Console.WriteLine("Starting init txs(both accounts start with 100$)");
    var tasks=  new List<Task>();
    var task1 = actor0.StartTransaction("Init", new Tuple<int, string>(actorId0, regionAndServer0), actorAccessInfo0);
    var task2 = actor1.StartTransaction("Init", new Tuple<int, string>(actorId1, regionAndServer1), actorAccessInfo1);
    tasks.Add(task1);
    tasks.Add(task2);
    await Task.WhenAll(tasks);

    Console.WriteLine("Starting deposit txs");

    var multiTransferInput = new Tuple<int, List<Tuple<int, string>>>(
        amountToDeposit,
        new List<Tuple<int, string>>() { new Tuple<int, string>(actorId1, regionAndServer1)
    });  // money, List<to account>
    await actor0.StartTransaction("MultiTransfer", multiTransferInput, actorAccessInfoForMultiTransfer);

    Console.WriteLine("Starting balance txs");

    var PACT_balance3 = await actor0.StartTransaction("Balance", null, actorAccessInfo0);
    Console.WriteLine($"The PACT balance in actor {actorId0} after giving money: Expected: 50, Actual:{PACT_balance3.resultObj}");

    var PACT_balance4 = await actor1.StartTransaction("Balance", null, actorAccessInfo1);
    Console.WriteLine($"The PACT balance in actor {actorId1} after receiving money: Expected: 150, Actual:{PACT_balance4.resultObj}");
  } catch (Exception e) {
     Console.WriteLine(e.Message);
     Console.WriteLine(e.StackTrace);
  }

Console.WriteLine("Ended deterministic tx");