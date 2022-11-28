using Concurrency.Implementation.Performance;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Utilities;

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

            var client = CreateClusterClient(deploymentType);
            ExperimentRunner experimentRunner = new ExperimentRunner();

            // await experimentRunner.ManyMultiTransferTransactions(client, region, multitransfers);
            if("StressLocal".Equals(function, StringComparison.CurrentCultureIgnoreCase))
            {
                int silos = int.Parse(args[3]);
                int grainsPerSilo = int.Parse(args[4]);
                int transactionSize = int.Parse(args[5]);

                await experimentRunner.StressRun(client, region, silos, grainsPerSilo, transactionSize);
            }
            else if("Stress".Equals(function, StringComparison.CurrentCultureIgnoreCase))
            {
                int silos = int.Parse(args[3]);
                int grainsPerSilo = int.Parse(args[4]);
                int transactionSize = int.Parse(args[5]);
                int runs = 1;

                const string MultiTransfer = "MultiTransfer";

                double averagePrepateTimeMultiTransfer = 0;
                double averageExecutionTimeMultiTransfer = 0;
                double averageCommitTimeMultiTransfer = 0;

                double averagePrepateTimeMultiTransferReplica = 0;
                double averageExecutionTimeMultiTransferReplica = 0;
                double averageCommitTimeMultiTransferReplica = 0;

                double averageLatencyMultiTransfer = 0;

                int multiTransfers = 0;

                await client.Connect();

                for (int i = 0; i < runs; i++)
                {
                    await experimentRunner.StressRun(client, region, silos, grainsPerSilo, transactionSize);

                    await Task.Delay(3000);

                    var performanceGrain = client.GetGrain<IPerformanceGrain>(0, "US");

                    multiTransfers = await performanceGrain.NumberOfTransactions(MultiTransfer);

                    averagePrepateTimeMultiTransfer += await performanceGrain.GetAveragePrepareTime(MultiTransfer);
                    averageExecutionTimeMultiTransfer += await performanceGrain.GetAverageExecutionTime(MultiTransfer);
                    averageCommitTimeMultiTransfer += await performanceGrain.GetAverageCommitTime(MultiTransfer);

                    // averagePrepateTimeMultiTransferReplica += await performanceGrain.GetAveragePrepareTimeReplica(MultiTransfer, US);
                    // averageExecutionTimeMultiTransferReplica += await performanceGrain.GetAverageExecutionTimeReplica(MultiTransfer, US);
                    // averageCommitTimeMultiTransferReplica += await performanceGrain.GetAverageCommitTimeReplica(MultiTransfer, US);

                    // averageLatencyMultiTransfer += await performanceGrain.GetAverageLatencyTime(MultiTransfer, US);

                    await performanceGrain.CleanUp();
                }

                Console.WriteLine($"TransactionSize: {transactionSize}");
                Console.WriteLine($"LocalCoordinators: {Constants.NumberOfLocalCoordinatorsPerSilo}");
                Console.WriteLine($"RegionalCoordinators: {Constants.NumberOfRegionalCoordinators}");
                Console.WriteLine($"Number of multitransfer transactions: {multiTransfers}");

                Console.WriteLine("");
                Console.WriteLine("###############################################");
                Console.WriteLine("");

                Console.WriteLine($"AveragePrepareTime MultiTransfer: {averagePrepateTimeMultiTransfer / runs}");
                Console.WriteLine($"AverageExecutionTime MultiTransfer: {averageExecutionTimeMultiTransfer / runs}");
                Console.WriteLine($"AverageCommitTime MultiTransfer: {averageCommitTimeMultiTransfer / runs}");

                Console.WriteLine("");
                Console.WriteLine("###############################################");
                Console.WriteLine("");

                Console.WriteLine($"AveragePrepareTime Replica MultiTransfer: {averagePrepateTimeMultiTransferReplica / runs}");
                Console.WriteLine($"AverageExecutionTime Replica MultiTransfer US: {averageExecutionTimeMultiTransferReplica / runs}");
                Console.WriteLine($"AverageCommitTime Replica MultiTransfer: {averageCommitTimeMultiTransferReplica / runs}");

                Console.WriteLine("");
                Console.WriteLine("###############################################");
                Console.WriteLine("");

                Console.WriteLine($"AverageLatency MultiTransfer US: {averageLatencyMultiTransfer / runs}");

                await client.Close();

            }
            else if("Correctness".Equals(function, StringComparison.CurrentCultureIgnoreCase))
            {

                int silos = int.Parse(args[3]);
                int grainsPerSilo = int.Parse(args[4]);

                await experimentRunner.CorrectnessCheck(client, region, silos, grainsPerSilo);
            }
            else
            {
                int multitransfers = int.Parse(args[3]);
                await experimentRunner.ManyMultiTransferTransactions(client, region, multitransfers);
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