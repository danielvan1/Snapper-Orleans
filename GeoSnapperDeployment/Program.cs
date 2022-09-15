using GeoSnapperDeployment.Models;
using Microsoft.Extensions.Configuration;
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

            UnityContainer container = new UnityContainer();
            container.RegisterType<ISiloInfoFactory, SiloInfoFactory>(TypeLifetime.Singleton);
            container.RegisterType<DeploySiloLocalDevelopmentEnvironment>(TypeLifetime.Singleton);
            container.RegisterType<ClusterDeployment>(TypeLifetime.Singleton);

            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(Configurations, "ClusterConfigurations.json"))
            .AddJsonFile(Path.Combine(Configurations, "SiloConfigurations.json"))
            .Build();

            string deploymentType = args[0];
            var siloHosts = new List<ISiloHost>();

            if(deploymentType.Equals("LocalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                var deployLocalDevelopmentEnvironment = container.Resolve<DeploySiloLocalDevelopmentEnvironment>();

                var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();
                Console.WriteLine($"port: {siloConfigurations.StartGatewayPort}");

                IList<ISiloHost> replicaSiloHosts = await deployLocalDevelopmentEnvironment.DeploySilosAndReplicas(siloConfigurations);
                siloHosts.AddRange(replicaSiloHosts);

                var globalSiloHost = await deployLocalDevelopmentEnvironment.DeployGlobalSilo(siloConfigurations);
                siloHosts.Add(globalSiloHost);
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