using System;
using Concurrency.Implementation.Configuration;
using Concurrency.Implementation.Coordinator;
using Concurrency.Interface.Coordinator;
using Moq;
using Orleans.TestingHost;
using SnapperGeoReplication.Tests.ClusterSetup;
using Xunit;

namespace SnapperGeoReplication.Tests;

[Collection(ClusterCollection.Name)]
public class LocalSingleHomeTests
{
    private readonly ClusterFixture fixture;
    private readonly TestCluster cluster;

    public LocalSingleHomeTests(ClusterFixture fixture)
    {
        this.fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));
        this.fixture.AddSiloBuilderConfigurator<TestLocalSiloConfiguration>();
        this.fixture.BuildAndDeployCluster();
        cluster = this.fixture.Cluster;
    }

    public void SuckADick()
    {
        var client = this.cluster.Client;

        var localCoordinatorGrainMock = new Mock<ILocalCoordinatorGrain>();
        // localCoordinatorGrainMock.Setup(x => x.NewTransaction)


        // var localConfigurationGrainMock = new Mock<LocalConfigurationGrain>();

        // localConfigurationGrainMock.Setup(x => x.GrainFactory.GetGrain<ILocalCoordinatorGrain>(0, "EU-EU-0", null))
        //                            .Returns(localCoordinatorGrainMock.Object);
    }
}