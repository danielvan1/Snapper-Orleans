using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;
using Concurrency.Interface.Configuration;
using Utilities;

var client = new ClientBuilder()
.UseLocalhostClustering()
.Configure<ClusterOptions>(options =>
{
    options.ClusterId = "ec2";
    options.ServiceId = "Snapper";
})
.Build();

await client.Connect();

var globalConfigGrain = client.GetGrain<IGlobalConfigGrain>(0);
await globalConfigGrain.ConfigGlobalEnv();

var actorId = 0;

var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId, Constants.PlaceholderKeyExtension);


// Required for the Tx 
Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

//API we want to call:
//Task<TransactionResult> StartTransaction(string startFunc, 
//                                         object funcInput,
//                                         List<int> grainAccessInfo,
//                                         List<string> grainClassName);

var actorAccessInfo = new List<int>();
actorAccessInfo.Add(actorId); // it takes 1 call to Init to finish this tx?

var grainClassName = new List<string>();                                             // grainID, grainClassName
grainClassName.Add(snapperTransactionalAccountGrainTypeName);

var initialBalance = 100;

Console.WriteLine("Started deterministic tx");
try {
    var PACT_balance = await actor.StartTransaction("Init", initialBalance, actorAccessInfo, grainClassName);
    Console.WriteLine("The PACT balance:"+PACT_balance);
} catch (Exception e) {
    Console.WriteLine(e.Message);
}

Console.WriteLine("Ended deterministic tx");