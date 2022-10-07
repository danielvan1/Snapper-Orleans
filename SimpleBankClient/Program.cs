using Concurrency.Implementation;
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
var replicaRegionAndServer = "US-EU-0";

var actorAccessInfo0 = new List<GrainAccessInfo>()
{
    new GrainAccessInfo()
    {
        Id = actorId0,
        Region = regionAndServer,
        GrainClassName = snapperTransactionalAccountGrainTypeName
    },
};

var replicaAccessInfo0 = new List<GrainAccessInfo>()
{
    new GrainAccessInfo()
    {
        Id = actorId0,
        Region = replicaRegionAndServer,
        GrainClassName = snapperTransactionalAccountGrainTypeName
    },
};

var actorAccessInfo1 = new List<Tuple<int, string>>()
{
    new Tuple<int, string>(actorId1, regionAndServer),
};

var grainClassName = new List<string>();
grainClassName.Add(snapperTransactionalAccountGrainTypeName);

var actor0 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer);
var replicaActor0 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, replicaRegionAndServer);
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

try {
    Console.WriteLine("Starting init txs(both accounts start with 100$)");
    await actor0.StartTransaction("Init", new Tuple<int, string>(actorId0, regionAndServer), actorAccessInfo0);
    await actor0.StartTransaction("Deposit", 5, actorAccessInfo0);


    // await actor1.StartTransaction("Init", new Tuple<int, string>(actorId1, regionAndServer), actorAccessInfo1, grainClassName);

    // Console.WriteLine("Starting deposit txs");

    // await actor0.StartTransaction("MultiTransfer", multiTransferInput, actorAccessInfoForMultiTransfer, grainClassNamesForMultiTransfer);

    // Console.WriteLine("Starting balance txs");

    var PACT_balance3 = await actor0.StartTransaction("Balance", null, actorAccessInfo0);
    var PACT_balance4 = await replicaActor0.StartTransaction("Balance", null, replicaAccessInfo0);
    Console.WriteLine($"{PACT_balance3.resultObj}, replica: {PACT_balance4.resultObj}");
    // Console.WriteLine($"{PACT_balance3.resultObj}");

    // var PACT_balance4 = await actor1.StartTransaction("Balance", null, actorAccessInfo1, grainClassName);
    // Console.WriteLine($"The PACT balance in actor {actorId1} after receiving money: Expected: 150, Actual:{PACT_balance4.resultObj}");
  }
  catch (Exception e)
  {
     Console.WriteLine(e.Message);
     Console.WriteLine(e.StackTrace);
  }

Console.WriteLine("Ended deterministic tx");