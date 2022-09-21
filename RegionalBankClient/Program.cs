using Concurrency.Interface.Configuration;
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

Console.WriteLine("Regional Bank Client is ready");

/*IGlobalConfigurationGrain globalConfigGrain = client.GetGrain<IGlobalConfigurationGrain>(0);
await globalConfigGrain.InitializeGlobalCoordinators();*/

IRegionalConfigGrain regionalConfigGrainEU = client.GetGrain<IRegionalConfigGrain>(0, "EU");
IRegionalConfigGrain regionalConfigGrainUS = client.GetGrain<IRegionalConfigGrain>(1, "US");
//IRegionalConfigGrain regionalConfigGrainUS = client.GetGrain<IRegionalConfigGrain>(0, "US");
// await regionalConfigGrainUS.InitializeRegionalCoordinators("US");

await regionalConfigGrainEU.InitializeRegionalCoordinators("EU");
await regionalConfigGrainEU.InitializeRegionalCoordinators("US");

ILocalConfigGrain localConfigGrainEU = client.GetGrain<ILocalConfigGrain>(3, "EU");
ILocalConfigGrain localConfigGrainUS = client.GetGrain<ILocalConfigGrain>(3, "US");
await localConfigGrainEU.InitializeLocalCoordinators("EU");
await localConfigGrainUS.InitializeLocalCoordinators("US");

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


var actorAccessInfo0 = new List<Tuple<int, string>>() 
{
    new Tuple<int, string>(actorId0, regionAndServer0),
};

var actorAccessInfo1 = new List<Tuple<int, string>>() 
{
    new Tuple<int, string>(actorId1, regionAndServer1),
};

var grainClassName = new List<string>();
grainClassName.Add(snapperTransactionalAccountGrainTypeName);

var actor0 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer0);
var accountId = actorId1;

var actor1 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer1);

var grainClassNamesForMultiTransfer = new List<string>();                                             // grainID, grainClassName
grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);
grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);

var actorAccessInfoForMultiTransfer = new List<Tuple<int, string>>() 
{
    new Tuple<int, string>(actorId0, regionAndServer0),
    new Tuple<int, string>(actorId1, regionAndServer1),
};

var amountToDeposit = 50;

try {
    Console.WriteLine("Starting init txs(both accounts start with 100$)");
    await actor0.StartTransaction("Init", new Tuple<int, string>(actorId0, regionAndServer0), actorAccessInfo0, grainClassName);
    await actor1.StartTransaction("Init", new Tuple<int, string>(actorId1, regionAndServer1), actorAccessInfo1, grainClassName);

    Console.WriteLine("Starting deposit txs");

    var multiTransferInput = new Tuple<int, List<Tuple<int, string>>>(
        amountToDeposit,
        new List<Tuple<int, string>>() { new Tuple<int, string>(actorId1, regionAndServer1) 
    });  // money, List<to account>
    await actor0.StartTransaction("MultiTransfer", multiTransferInput, actorAccessInfoForMultiTransfer, grainClassNamesForMultiTransfer);

    Console.WriteLine("Starting balance txs");

    var PACT_balance3 = await actor0.StartTransaction("Balance", null, actorAccessInfo0, grainClassName);
    Console.WriteLine($"The PACT balance in actor {actorId0} after giving money: Expected: 50, Actual:{PACT_balance3.resultObj}");

    var PACT_balance4 = await actor1.StartTransaction("Balance", null, actorAccessInfo1, grainClassName);
    Console.WriteLine($"The PACT balance in actor {actorId1} after receiving money: Expected: 150, Actual:{PACT_balance4.resultObj}");
  } catch (Exception e) {
     Console.WriteLine(e.Message);
     Console.WriteLine(e.StackTrace);
  }

Console.WriteLine("Ended deterministic tx");