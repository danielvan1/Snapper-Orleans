using System;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.TransactionExecution;
using Orleans.TestingHost;
using SnapperGeoReplication.Tests.ClusterSetup;
using Xunit;

namespace SnapperGeoReplication.Tests;

[Collection(ClusterCollection.Name)]
public class UnitTest1
{
    private readonly ClusterFixture fixture;
    private readonly TestCluster cluster;

    public UnitTest1(ClusterFixture fixture)
    {
        this.fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));
        this.fixture.AddSiloBuilderConfigurator<TestLocalSiloConfiguration>();
        this.fixture.BuildAndDeployCluster();
        cluster = this.fixture.Cluster;
    }

    [Fact]
    public void Test1()
    {
        var regionalCoordinator = cluster.GrainFactory.GetGrain<ITransactionExecutionGrain>(1, "EU");
    }
}