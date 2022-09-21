using Xunit;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    [CollectionDefinition(ClusterCollection.Name)]

    public class ClusterCollection
    {
        public const string Name = "ClusterCollection";
    }
}