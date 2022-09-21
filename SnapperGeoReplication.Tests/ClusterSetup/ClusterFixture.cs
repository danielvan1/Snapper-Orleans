using System;
using Orleans.TestingHost;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class ClusterFixture : IDisposable
    {
        private TestClusterBuilder builder;

        public ClusterFixture()
        {
            this.builder = new TestClusterBuilder();
        }

        public void AddSiloBuilderConfigurator<T>() where T : ISiloConfigurator, new()
        {
            this.builder.AddSiloBuilderConfigurator<T>();
        }

        public void BuildAndDeployCluster()
        {
            this.Cluster = builder.Build();
            this.Cluster.Deploy();
        }

        public void Dispose()
        {
            this.Cluster.StopAllSilos();
        }

        public TestCluster Cluster { get; private set; }
    }
}