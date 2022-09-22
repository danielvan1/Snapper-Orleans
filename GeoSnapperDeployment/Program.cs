using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.VisualBasic;
using Orleans.Hosting;
using Unity;
using Utilities;

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
            container.RegisterType<ISiloConfigurationFactory, SiloConfigurationFactory>(TypeLifetime.Singleton);
            container.RegisterType<LocalSiloDeployer>(TypeLifetime.Singleton);

            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(Configurations, "SiloConfigurations.json"))
            .Build();

            string deploymentType = args[0];
            var siloHosts = new List<ISiloHost>();

            if(deploymentType.Equals("LocalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                var localSiloDeployer = container.Resolve<LocalSiloDeployer>();

                var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();

                var primarySiloHost = await localSiloDeployer.DeployPrimarySilo(siloConfigurations);
                siloHosts.Add(primarySiloHost);

                var globalSiloHost = await localSiloDeployer.DeployGlobalSilo(siloConfigurations);
                siloHosts.Add(globalSiloHost);

                IList<ISiloHost> regionSiloHosts = await localSiloDeployer.DeployRegionalSilos(siloConfigurations);
                siloHosts.AddRange(regionSiloHosts);

                IList<ISiloHost> localSiloHosts = await localSiloDeployer.DeploySilosAndReplicas(siloConfigurations);
                siloHosts.AddRange(localSiloHosts);
            }
            else
            {
                throw new ArgumentException($"Invalid deployment type given: {deploymentType}");
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