using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Hosting;
using Orleans.TestingHost;
using System;

namespace SnapperGeoReplication.Tests.ClusterSetup
{
    public class TestLocalSiloConfiguration : ISiloConfigurator, IDisposable
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            Mock<ILogger> loggerMock = new Mock<ILogger>();

            siloBuilder.ConfigureServices(serviceCollection =>
            {
                serviceCollection.AddSingleton<ILogger>(loggerMock.Object);
            });
        }

        public void Dispose() { }
    }
}