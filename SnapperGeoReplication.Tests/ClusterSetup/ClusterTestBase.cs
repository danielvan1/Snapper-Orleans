using System;
using Orleans.TestingHost;
using Xunit;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    /// <summary>
    /// An abstract class containing a <see cref="TestCluster"/> for use in tests against
    /// the Orleans grains.
    /// </summary>
    public abstract class ClusterTestBase<T> where T: ISiloConfigurator, IDisposable, new()
    {
        protected readonly TestCluster Cluster;

        protected ClusterTestBase(ClusterFixture<T> clusterFixture)
        {
            Cluster = clusterFixture.Cluster;
        }
    }

}