using System;
using Orleans.Hosting;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using SnapperSiloHost.Models;
using System.Collections.Generic;
using Unity;

namespace SnapperSiloHost
{
    public class Program
    {
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
            container.RegisterType<DeployLocalDevelopmentEnvironment>(TypeLifetime.Singleton);

            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

            string deploymentType = args[0];
            var siloHosts = new List<ISiloHost>();

            if(deploymentType.Equals("LocalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                var deployLocalDevelopmentEnvironment = container.Resolve<DeployLocalDevelopmentEnvironment>();
                var localDeployment = config.GetRequiredSection("LocalDeployment").Get<LocalDeployment>();
                Console.WriteLine($"port: {localDeployment.StartGatewayPort}");

                IList<ISiloHost> replicaSiloHosts = await deployLocalDevelopmentEnvironment.DeploySilosAndReplicas(localDeployment);
                siloHosts.AddRange(replicaSiloHosts);

                var globalSiloHost = await deployLocalDevelopmentEnvironment.DeployGlobalSilo(localDeployment);
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