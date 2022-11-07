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
            string function = args[2];

            using(var client = CreateClusterClient(deploymentType))
            {
                ExperimentRunner experimentRunner = new ExperimentRunner();

                // await experimentRunner.ManyMultiTransferTransactions(client, region, multitransfers);
                if("Stress".Equals(function, StringComparison.CurrentCultureIgnoreCase))
                {
                    int silos = int.Parse(args[3]);
                    int grainsPerSilo = int.Parse(args[4]);

                    await experimentRunner.StressRun(client, region, silos, grainsPerSilo);
                }
                else
                {

                    int multitransfers = int.Parse(args[3]);
                    await experimentRunner.ManyMultiTransferTransactions(client, region, multitransfers);
                }
            }
        }

        private static IClusterClient CreateClusterClient(string deploymentType)
        {
            var clientBuilder = new ClientBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "Snapper";
                    options.ServiceId = "Snapper";
                })
                .Configure<ClientMessagingOptions>(options =>
                {
                    options.ResponseTimeout = new TimeSpan(0, 5, 0);
                });

            if(deploymentType.Equals("LocalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                clientBuilder.UseLocalhostClustering();
            }
            else if(deploymentType.Equals("GlobalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                const string key1 = "DefaultEndpointsProtocol=https;AccountName=snapperstorage;AccountKey=OYoqvb955xUGAu9SkZEMapbNAxl3vN3En2wNqVQV6iEmZE4UWCydMFL/cO+78QvN0ufhxWZNlZIA+AStQx1IXQ==;EndpointSuffix=core.windows.net";
                clientBuilder.UseAzureStorageClustering(options => options.ConfigureTableServiceClient(key1));
            }

            return clientBuilder.Build();
        }
    }
}