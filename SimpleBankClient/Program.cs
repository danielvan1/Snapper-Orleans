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
    options.ClusterId = "EU";
    options.ServiceId = "EU";
})
.Build();

await client.Connect();

IGlobalConfigurationGrain globalConfigGrain = client.GetGrain<IGlobalConfigurationGrain>(0);

await globalConfigGrain.InitializeGlobalCoordinators();

// var actorId1 = 1;
// var actorId2 = 2;

// var actor1 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, Constants.US);
// var actor2 = client.GetGrain<ISnapperTransactionalAccountGrain>(actorId2, Constants.US);


// // Required for the Txs(All ISnapperTransactionalAccountGrain's TXs can reuse this): 
// Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
// string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

//API we want to call:
//Task<TransactionResult> StartTransaction(string startFunc, 
//                                         object funcInput,
//                                         List<int> grainAccessInfo,
//                                         List<string> grainClassName);

// Required setup for the Initial TXs giving both an initial balance of 100
// Remember that we just assume that each actor is only called ONCE in a tx now
// I don't think there is anyway to have TXs involving multiple calls to one
// actor
// var actorAccessInfo1 = new List<int>();
// actorAccessInfo1.Add(actorId1);

// var actorAccessInfo2 = new List<int>();
// actorAccessInfo1.Add(actorId1);

// var grainClassName = new List<string>();                                             // grainID, grainClassName
// grainClassName.Add(snapperTransactionalAccountGrainTypeName);

// var initialBalance = 100;

// // Required setup for starting transactions that deposits funds between two actors
// var actorAccessInfoForDeposit = new List<int>();
// actorAccessInfoForDeposit.Add(actorId1);
// actorAccessInfoForDeposit.Add(actorId2);

// var grainClassNamesForDeposit = new List<string>();                                             // grainID, grainClassName
// grainClassNamesForDeposit.Add(snapperTransactionalAccountGrainTypeName);
// grainClassNamesForDeposit.Add(snapperTransactionalAccountGrainTypeName);

// var amountToDeposit = 50;

// Console.WriteLine("Started deterministic tx");
// try {
//     var PACT_balance1 = await actor1.StartTransaction("Init", initialBalance, actorAccessInfo1, grainClassName);
//     Console.WriteLine("The PACT balance in actor 1:"+PACT_balance1);
//     var PACT_balance2 = await actor2.StartTransaction("Init", initialBalance, actorAccessInfo2, grainClassName);
//     Console.WriteLine("The PACT balance in actor 2:"+PACT_balance2);

//     await actor1.StartTransaction("Deposit", amountToDeposit, actorAccessInfoForDeposit, grainClassNamesForDeposit);
//     // TODO: Figure out if `null` is the correct argument for Balance
//     var PACT_balance3 = await actor1.StartTransaction("Balance", null, actorAccessInfo1, grainClassName);
//     Console.WriteLine("The PACT balance in actor 1 after deposit(Expected 50):"+PACT_balance3);
//     var PACT_balance4 = await actor2.StartTransaction("Balance", null, actorAccessInfo2, grainClassName);
//     Console.WriteLine("The PACT balance in actor 2 after deposit(Expected 150):"+PACT_balance4);
// } catch (Exception e) {
//     Console.WriteLine(e.Message);
// }

// Console.WriteLine("Ended deterministic tx");