using Xunit;
using System;
using Orleans.TestingHost;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    [CollectionDefinition(Name)]
    public class SimpleRegionalClusterCollection : ICollectionFixture<ClusterFixture<RegionalIntegrationTestConfiguration>> 
    {
        public const string Name = nameof(SimpleRegionalClusterCollection);
    }
}