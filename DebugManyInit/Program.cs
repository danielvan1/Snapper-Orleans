using Concurrency.Interface.Configuration;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using SmallBank.Interfaces;
using System.Threading.Tasks;
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

Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

var regionAndServer = "EU-EU-0";
var regionAndServer1 = "EU-EU-1";


var grainClassName = new List<string>();
grainClassName.Add(snapperTransactionalAccountGrainTypeName);

try {
    Console.WriteLine("Starting init txs(all accounts start with 10000$)");
    var tasks = new List<Task<TransactionResult>>();
    var n = 1000;
    for(int i = 0; i < n; i++) {
        if ( i > n/2 ) {
            regionAndServer = regionAndServer1;
        }

        var actorAccessInfo = new List<Tuple<int, string>>() 
        {
            new Tuple<int, string>(i, regionAndServer),
        };

        var actor = client.GetGrain<ISnapperTransactionalAccountGrain>(i, regionAndServer);
        var task = actor.StartTransaction("Init", new Tuple<int, string>(i, regionAndServer), actorAccessInfo, grainClassName);
        tasks.Add(task);
    }
    await Task.WhenAll(tasks);
    Console.WriteLine("Done with init txs");
  }
  catch (Exception e)
  {
     Console.WriteLine(e.Message);
     Console.WriteLine(e.StackTrace);
  }
