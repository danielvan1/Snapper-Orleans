﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Net;
using AccountTransfer.Grains;
using Concurrency.Implementation.Deterministic;
using Concurrency.Implementation.Nondeterministic;
using Utilities;

namespace OrleansSiloHost
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                //var host = await StartSilo();
                var host = await StartClusterSilo();
                Console.WriteLine("Press Enter to terminate...");
                Console.ReadLine();

                await host.StopAsync();

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }

        private static async Task<ISiloHost> StartSilo()
        {
            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "AccountTransferApp";
                   
                    
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(DeterministicTransactionCoordinator).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(NondeterministicTransactionCoordinator).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(ATMGrain).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(AccountGrain).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));
            
            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static async Task<ISiloHost> StartClusterSilo()
        {

            Action<DynamoDBClusteringOptions> dynamoDBOptions = options => {
                options.AccessKey = "AKIAJILO2SVPTNUZB55Q";
                options.SecretKey = "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";
                options.TableName = "XLibMembershipTable";
                options.Service = "eu-west-1";
                options.WriteCapacityUnits = 10;
                options.ReadCapacityUnits = 10;

            };

            var builder = new SiloHostBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "AccountTransferApp";
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(Helper.GetLocalIPAddress()))
                .UseDynamoDBClustering(dynamoDBOptions)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(DeterministicTransactionCoordinator).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(NondeterministicTransactionCoordinator).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(ATMGrain).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(AccountGrain).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            var host = builder.Build();
            
            await host.StartAsync();
            return host;
        }
    }
}
