using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Configuration;
using Orleans.TestingHost;
using Xunit;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class ClusterFixture<T> : IAsyncLifetime where T : ISiloConfigurator,IDisposable, new()
    {
        private TestClusterBuilder builder;

        // Method given by IAsyncLifetime which allows async setup
        // Obviously our ClusterFixture loses some reusability
        // since not all of our tests need the same coordinators
        // in the same regions setup 
        public async Task InitializeAsync()
        {
            IRegionalConfigGrain regionalConfigGrainEU = this.Cluster.GrainFactory.GetGrain<IRegionalConfigGrain>(0, "EU");
            ILocalConfigGrain localConfigGrainEU = this.Cluster.GrainFactory.GetGrain<ILocalConfigGrain>(3, "EU");

            var task1 = regionalConfigGrainEU.InitializeRegionalCoordinators("EU");
            var task2 = localConfigGrainEU.InitializeLocalCoordinators("EU");
            List<Task> configureAllConfigAndCoordinators = new List<Task>()
            {
                task1, task2
            };

            try {
                await Task.WhenAll(configureAllConfigAndCoordinators);
            } catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        public ClusterFixture()
        {
            this.builder = new TestClusterBuilder();
            this.builder.AddSiloBuilderConfigurator<T>();
            Cluster = builder.Build();
            Cluster.Deploy();
        }

        public void Dispose()
        {
            this.Cluster.StopAllSilos();
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        public TestCluster Cluster { get; private set; }
    }
}