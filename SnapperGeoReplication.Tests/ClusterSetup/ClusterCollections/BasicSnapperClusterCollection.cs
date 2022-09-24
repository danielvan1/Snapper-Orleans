using Xunit;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    [CollectionDefinition(Name)]
    public class BasicSnapperClusterCollection : ICollectionFixture<ClusterFixture<RegionalIntegrationTestConfiguration>> 
    {
        public const string Name = nameof(BasicSnapperClusterCollection);
    }
}