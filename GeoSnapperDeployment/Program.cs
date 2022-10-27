using System.Net;
using Concurrency.Interface.Configuration;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Unity;

namespace GeoSnapperDeployment
{
    public class Program
    {
        private const string Configurations = "Configurations";

        public static int Main(string[] args)
        {
            return MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task<int> MainAsync(string[] args)
        {
            if(args.Length == 0)
            {
                throw new ArgumentException("Deployment type needs to be specified");
            }

            if(!Directory.Exists(Utilities.Constants.LogPath))
            {
                Directory.CreateDirectory(Utilities.Constants.LogPath);
            }

            UnityContainer container = new UnityContainer();
            container.RegisterType<ISiloInfoFactory, SiloInfoFactory>(TypeLifetime.Singleton);
            container.RegisterType<ISiloConfigurationFactory, SiloConfigurationForLocalDeploymentFactory>(TypeLifetime.Singleton);
            container.RegisterType<ISiloConfigurationForGlobalDeployment, SiloConfigurationForGlobalDeployment>(TypeLifetime.Singleton);
            container.RegisterType<LocalSiloDeployer>(TypeLifetime.Singleton);
            container.RegisterType<GlobalSiloDeployer>(TypeLifetime.Singleton);


            string deploymentType = args[0];
            var siloHosts = new List<ISiloHost>();

            if(deploymentType.Equals("LocalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile(Path.Combine(Configurations, "SiloConfigurations.json"))
                .Build();

                var localSiloDeployer = container.Resolve<LocalSiloDeployer>();

                var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();

                var primarySiloHost = await localSiloDeployer.DeployPrimarySilo(siloConfigurations);
                siloHosts.Add(primarySiloHost);

                var globalSiloHost = await localSiloDeployer.DeployGlobalSilo(siloConfigurations);
                siloHosts.Add(globalSiloHost);

                IList<ISiloHost> regionSiloHosts = await localSiloDeployer.DeployRegionalSilos(siloConfigurations);
                siloHosts.AddRange(regionSiloHosts);

                IList<ISiloHost> localSiloHosts = await localSiloDeployer.DeployLocalSilosAndReplicas(siloConfigurations);
                siloHosts.AddRange(localSiloHosts);

                var client = new ClientBuilder()
                .UseLocalhostClustering()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "Snapper";
                    options.ServiceId = "Snapper";
                })
                .Configure<ClientMessagingOptions>(options =>
                {
                    options.ResponseTimeout = new TimeSpan(0, 5, 0);
                })
                .Build();

                await client.Connect();

                IRegionalCoordinatorConfigGrain regionalConfigGrainEU = client.GetGrain<IRegionalCoordinatorConfigGrain>(0, "EU");
                IRegionalCoordinatorConfigGrain regionalConfigGrainUS = client.GetGrain<IRegionalCoordinatorConfigGrain>(1, "US");

                await regionalConfigGrainEU.InitializeRegionalCoordinators("EU");
                await regionalConfigGrainUS.InitializeRegionalCoordinators("US");

                ILocalConfigGrain localConfigGrainEU = client.GetGrain<ILocalConfigGrain>(3, "EU");
                ILocalConfigGrain localConfigGrainUS = client.GetGrain<ILocalConfigGrain>(3, "US");
                await localConfigGrainEU.InitializeLocalCoordinators("EU");
                await localConfigGrainUS.InitializeLocalCoordinators("US");

                await client.Close();

            }
            else if(deploymentType.Equals("GlobalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                string region = args[1];

                IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile(Path.Combine(Configurations, "GlobalSiloConfigurations.json"))
                .Build();

                var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();
                Console.WriteLine($"region: {region}");
                Console.WriteLine($"siloConfigurations: {siloConfigurations.ClusterId}");
                Console.WriteLine($"siloConfigurations: {siloConfigurations.Silos.PrimarySilo.IPAddress}");

                var globalSiloDeployer = container.Resolve<GlobalSiloDeployer>();

                siloHosts.AddRange(await globalSiloDeployer.Deploy(siloConfigurations, region));

                const string key1 = "DefaultEndpointsProtocol=https;AccountName=snapperstorage;AccountKey=OYoqvb955xUGAu9SkZEMapbNAxl3vN3En2wNqVQV6iEmZE4UWCydMFL/cO+78QvN0ufhxWZNlZIA+AStQx1IXQ==;EndpointSuffix=core.windows.net";

                var client = new ClientBuilder()
                .UseAzureStorageClustering(options => options.ConfigureTableServiceClient(key1))
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = siloConfigurations.ClusterId;
                    options.ServiceId = siloConfigurations.ServiceId;
                })
                .Configure<ClientMessagingOptions>(options =>
                {
                    options.ResponseTimeout = new TimeSpan(0, 5, 0);
                })
                .Build();

                await client.Connect();

                IRegionalCoordinatorConfigGrain regionalConfigGrainEU = client.GetGrain<IRegionalCoordinatorConfigGrain>(0, region);

                await client.Close();
            }

            Console.WriteLine("All silos created successfully");
            Console.WriteLine("Press Enter to terminate all silos...");
            Console.ReadLine();

            List<Task> stopSiloHostTasks = new List<Task>();

            foreach(ISiloHost siloHost in siloHosts)
            {
                stopSiloHostTasks.Add(siloHost.StopAsync());
            }

            await Task.WhenAll(stopSiloHostTasks);

            Console.WriteLine("Stopped all silos");



            return 0;
        }
    }
}