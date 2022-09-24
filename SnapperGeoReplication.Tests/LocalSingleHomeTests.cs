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
public class LocalSingleHomeTests : ClusterTestBase<TestLocalSiloConfiguration>
{
    public LocalSingleHomeTests() : base(new ClusterFixture<TestLocalSiloConfiguration>())
    {
    }

    public void SuckADick()
    {
        var client = this.Cluster.Client;

        var localCoordinatorGrainMock = new Mock<ILocalCoordinatorGrain>();
        // localCoordinatorGrainMock.Setup(x => x.NewTransaction)


        // var localConfigurationGrainMock = new Mock<LocalConfigurationGrain>();

        // localConfigurationGrainMock.Setup(x => x.GrainFactory.GetGrain<ILocalCoordinatorGrain>(0, "EU-EU-0", null))
        //                            .Returns(localCoordinatorGrainMock.Object);
    }
}