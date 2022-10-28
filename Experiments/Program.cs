using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Experiments
{
    public class Program
    {
        public static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        private static async Task MainAsync(string[] args)
        {
            string deploymentType = args[0];
            string region = args[1];
            int multitransfers = int.Parse(args[2]);

            if("LocalDeployment".Equals(args[0]))
            {
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
                    .UseLocalhostClustering()
                    .Build();

                ExperimentRunner experimentRunner = new ExperimentRunner();

                await experimentRunner.ManyMultiTransferTransactions(client, region, multitransfers);

            }
            else
            {
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

                ExperimentRunner experimentRunner = new ExperimentRunner();

                await experimentRunner.ManyMultiTransferTransactions(client, region, multitransfers);

            }
        }
    }
}