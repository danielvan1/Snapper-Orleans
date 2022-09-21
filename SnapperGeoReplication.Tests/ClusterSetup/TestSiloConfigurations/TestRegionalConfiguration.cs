using Amazon.Runtime.Internal.Util;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace SnapperGeoReplication.Tests.ClusterSetup.TestSiloConfigurations
{
    public class TestRegionalConfiguration : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            Mock<ILogger> loggerMock = new Mock<ILogger>();
            siloBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddSingleton<ILogger>(loggerMock.Object);
            });
        }
    }
}