using Concurrency.Interface;
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
var deployedRegionReplica = "US";
var deployedRegion = "EU";
var homeRegion = "EU";
var server0 = "0";
var server1 = "1";
var regionAndServer0 = $"{deployedRegion}-{homeRegion}-{server0}";
var regionAndServer0Replica = $"{deployedRegionReplica}-{homeRegion}-{server0}";
var regionAndServer1 = $"{deployedRegion}-{homeRegion}-{server1}";
var regionAndServer1Replica = $"{deployedRegionReplica}-{homeRegion}-{server1}";


var actorAccessInfo0 = new List<GrainAccessInfo>()
{
    new GrainAccessInfo()
    {
        Id = actorId0,
        Region = regionAndServer0,
        GrainClassName = snapperTransactionalAccountGrainTypeName
    }
};

var actorAccessInfo0Replica = new List<GrainAccessInfo>()
{
    new GrainAccessInfo()
    {
        Id = actorId0,
        Region = regionAndServer0Replica,
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

var actorAccessInfo1Replica = new List<GrainAccessInfo>()
{
    new GrainAccessInfo()
    {
        Id = actorId1,
        Region = regionAndServer1Replica,
        GrainClassName = snapperTransactionalAccountGrainTypeName
    }
};

var grainClassName = new List<string>();
grainClassName.Add(snapperTransactionalAccountGrainTypeName);

var actor0 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer0);
var actor0Replica = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer0Replica);
var accountId = actorId1;

var actor1 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer1);
var actor1Replica = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer1Replica);


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
    Console.WriteLine("Starting init txs(both accounts start with 100$)");
    var tasks = new List<Task>();
    FunctionInput initFunctionInput0 = FunctionInputHelper.Create(100, new Tuple<int, string>(actorId0, regionAndServer0));
    FunctionInput initFunctionInput1 = FunctionInputHelper.Create(100, new Tuple<int, string>(actorId1, regionAndServer1));
    var task1 = actor0.StartTransaction("Init", initFunctionInput0, actorAccessInfo0);
    var task2 = actor1.StartTransaction("Init", initFunctionInput1, actorAccessInfo1);
    tasks.Add(task1);
    tasks.Add(task2);
    await Task.WhenAll(tasks);

    Console.WriteLine("Starting deposit txs");

    FunctionInput multiTransferFunctionInput = FunctionInputHelper.Create(amountToDeposit, new Tuple<int, string>(actorId1, regionAndServer1));
    await actor0.StartTransaction("MultiTransfer", multiTransferFunctionInput, actorAccessInfoForMultiTransfer);

    Console.WriteLine("Starting balance txs");

    var PACT_balance3 = await actor0.StartTransaction("Balance", null, actorAccessInfo0);
    Console.WriteLine($"The PACT balance in actor {actorId0} after giving money: Expected: 50, Actual:{PACT_balance3.resultObj}");

    var PACT_balance3Replica = await actor0Replica.StartTransaction("Balance", null, actorAccessInfo0Replica);
    Console.WriteLine($"The PACT balance in actor {actorId0} after giving money: Expected: 50, Actual:{PACT_balance3Replica.resultObj}");

    var PACT_balance4 = await actor1.StartTransaction("Balance", null, actorAccessInfo1);
    Console.WriteLine($"The PACT balance in actor {actorId1} after receiving money: Expected: 150, Actual:{PACT_balance4.resultObj}");

    var PACT_balance4Replica = await actor1Replica.StartTransaction("Balance", null, actorAccessInfo1Replica);
    Console.WriteLine($"The PACT balance in actor {actorId1} after receiving money: Expected: 150, Actual:{PACT_balance4Replica.resultObj}");
}
catch (Exception e)
{
    Console.WriteLine(e.Message);
    Console.WriteLine(e.StackTrace);
}

Console.WriteLine("Ended deterministic tx");