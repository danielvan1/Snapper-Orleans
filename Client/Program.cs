using Concurrency.Interface.Configuration;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Client
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task<int> MainAsync(string[] args)
        {
            string region = args[0];

            const string key1 = "DefaultEndpointsProtocol=https;AccountName=snapperstorage;AccountKey=OYoqvb955xUGAu9SkZEMapbNAxl3vN3En2wNqVQV6iEmZE4UWCydMFL/cO+78QvN0ufhxWZNlZIA+AStQx1IXQ==;EndpointSuffix=core.windows.net";
            var client = new ClientBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "Snapper";
                    options.ServiceId = "Snapper";
                })
                .Configure<ClientMessagingOptions>(options =>
                {
                    options.ResponseTimeout = new TimeSpan(0, 5, 0);
                })
                .UseAzureStorageClustering(options => options.ConfigureTableServiceClient(key1))
                .Build();

            await client.Connect();

            IRegionalCoordinatorConfigGrain regionalConfigGrainEU = client.GetGrain<IRegionalCoordinatorConfigGrain>(0, region);
            ILocalConfigGrain localConfigGrainEU = client.GetGrain<ILocalConfigGrain>(0, region);

            await regionalConfigGrainEU.InitializeRegionalCoordinators(region);
            await localConfigGrainEU.InitializeLocalCoordinators(region);

            await client.Close();

            return 0;
        }
    }
}

