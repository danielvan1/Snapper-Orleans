using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Hosting;
using Orleans.TestingHost;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class TestLocalSiloConfiguration : ISiloConfigurator
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