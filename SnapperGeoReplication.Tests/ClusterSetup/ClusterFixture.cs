using System;
using Orleans.TestingHost;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class ClusterFixture<T> where T : ISiloConfigurator,IDisposable, new()
    {
        private TestClusterBuilder builder;

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

        public TestCluster Cluster { get; private set; }
    }
}