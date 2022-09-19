using Orleans.TestingHost;
using Xunit;

namespace SnapperGeoReplication.Tests;

public class UnitTest1
{
    [Fact]
    public void Test1()
    {
        var builder = new TestClusterBuilder();
        var cluster = builder.Build();
    }
}