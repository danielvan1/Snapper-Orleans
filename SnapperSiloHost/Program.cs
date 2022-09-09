﻿using System;
using Orleans;
using System.Net;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Configuration;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using Microsoft.Extensions.DependencyInjection;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.Coordinator;
using Microsoft.Extensions.Configuration;
using SnapperSiloHost.Models;
using System.Collections.Generic;

namespace SnapperSiloHost
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return RunMainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task<int> RunMainAsync(string[] args)
        {
            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build();

            string deploymentType = args[0];
            var startSiloHostTasks = new List<Task>();
            var siloHosts = new List<ISiloHost>();

            if(deploymentType.Equals("LocalDeployment", StringComparison.CurrentCultureIgnoreCase))
            {
                var localDeployment = config.GetRequiredSection("LocalDeployment").Get<LocalDeployment>();

                foreach(SiloInfo info in localDeployment.Silos)
                {
                    var siloHostBuilder = new SiloHostBuilder();
                    var siloHost = LocalDeployment(siloHostBuilder, localDeployment, info);

                    Console.WriteLine($"Silo {info.SiloId} is started...");

                    await siloHost.StartAsync();
                }
            }
            else
            {
                throw new ArgumentException($"Invalid deployment type given: {deploymentType}");
            }

            Console.WriteLine("All silos created successfully");
            Console.WriteLine("Press Enter to terminate all silos...");
            Console.ReadLine();

            foreach(ISiloHost siloHost in siloHosts)
            {
                await siloHost.StopAsync();
            }

            return 0;
        }

        private static ISiloHost LocalDeployment(SiloHostBuilder siloHostBuilder, LocalDeployment localDeployment, SiloInfo info)
        {
            // Primary silo is only needed for local deployment!
            var primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, localDeployment.PrimarySiloEndpoint);
            siloHostBuilder.UseDevelopmentClustering(primarySiloEndpoint)
                // The IP address used for clustering / to be advertised in membership tables
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback);

            siloHostBuilder.Configure<ClusterOptions>(options =>
            {
                options.ClusterId = localDeployment.ClusterId;
                options.ServiceId = localDeployment.ServiceId;
            });

            siloHostBuilder.Configure<EndpointOptions>(options =>
            {
                options.SiloPort = info.SiloPort;
                options.GatewayPort = info.GatewayPort;
            });

            // TODO: Maybe do not add all the configuration for global and local coordinators here? Instead only add one
            // of them depending on if it is a global or local silo.
            siloHostBuilder.ConfigureServices(ConfigureGlobalCoordinator)
                           .ConfigureServices(ConfigureLocalGrains);

            siloHostBuilder.AddMemoryGrainStorageAsDefault();

            return siloHostBuilder.Build();
        }

        private static void ConfigureGlobalCoordinator(IServiceCollection services)
        {
            services.AddSingleton<ILoggerGroup, LoggerGroup>();
            services.AddSingleton<ICoordMap, CoordMap>();

            services.AddSingletonNamedService<PlacementStrategy, GlobalConfigGrainPlacementStrategy>(nameof(GlobalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigGrainPlacement>(typeof(GlobalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));
        }

        private static void ConfigureLocalGrains(IServiceCollection services)
        {
            // all the singletons have one instance per silo host??
            services.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigGrainPlacement>(typeof(LocalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, LocalCoordGrainPlacementStrategy>(nameof(LocalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordGrainPlacement>(typeof(LocalCoordGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));

        }

    }
}