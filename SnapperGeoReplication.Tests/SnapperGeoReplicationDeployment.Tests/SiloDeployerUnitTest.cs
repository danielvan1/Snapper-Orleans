using System.Collections.Generic;
using System.IO;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.Configuration;
using Xunit;
using System.Linq;

namespace SnapperGeoReplication.Tests
{
    [Collection("Silo deployer configuration")]
    public class SiloDeployerUnitTest
    {

        public SiloDeployerUnitTest()
        {
        }

        [Fact]
        public void TestCorrectSiloAndReplicaRegionsAreBeingGenerated()
        {
            var configurationsPath = "Configurations";

            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(configurationsPath, "SiloConfigurationsTest1.json"))
            .Build();

            var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();

            var siloInfoFactory = new SiloInfoFactory();
            var siloConfigurationFactory = new SiloConfigurationFactory(siloInfoFactory);
            var siloPlacementInfo = siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);
            var localSiloInfos = siloPlacementInfo.LocalSiloInfo;

            var expectedSiloRegionsAndServers = new List<string>()
            {
                // EU homes
                "EU-EU-0",
                "EU-EU-1",
                // US homes
                "US-US-0",
                // US replicas in EU
                "EU-US-0",
                // EU replicas in US
                "US-EU-0",
                "US-EU-1",
            };

            Xunit.Assert.Equal(expectedSiloRegionsAndServers, localSiloInfos.Keys);
        }

        [Fact]
        public void TestCorrectSiloAndReplicaRegionsAreBeingGeneratedManyRegions()
        {
            var configurationsPath = "Configurations";

            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(configurationsPath, "SiloConfigurationsTest2.json"))
            .Build();

            var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();

            var siloInfoFactory = new SiloInfoFactory();
            var siloConfigurationFactory = new SiloConfigurationFactory(siloInfoFactory);
            var siloPlacementInfo = siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);
            var localSiloInfos = siloPlacementInfo.LocalSiloInfo;

            var expectedSiloRegionsAndServers = new List<string>() 
            {
                // EU homes
                "EU-EU-0",
                "EU-EU-1",
                // US homes
                "US-US-0",
                // US replicas in EU
                "EU-US-0",
                // EU replicas in US
                "US-EU-0",
                "US-EU-1",
            };

            Xunit.Assert.Equal(expectedSiloRegionsAndServers, localSiloInfos.Keys);
        }

        [Fact]
        public void TestCorrectSiloAndReplicaRegionsAreBeingGeneratedManyRegionsAndSilos()
        {
            var configurationsPath = "Configurations";

            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile(Path.Combine(configurationsPath, "SiloConfigurationsTest3.json"))
            .Build();

            var siloConfigurations = config.GetRequiredSection("SiloConfigurations").Get<SiloConfigurations>();

            var siloInfoFactory = new SiloInfoFactory();
            var siloConfigurationFactory = new SiloConfigurationFactory(siloInfoFactory);
            var siloPlacementInfo = siloConfigurationFactory.CreateLocalSilosDictionary(siloConfigurations);
            var localSiloInfos = siloPlacementInfo.LocalSiloInfo;

            var expectedSiloRegionsAndServers = new List<string>() 
            {
                // EU homes
                "EU-EU-0",
                "EU-EU-1",

                // ASIA homes
                "ASIA-ASIA-0",

                // SOUTH-AMERICA homes
                "SOUTH-AMERICA-SOUTH-AMERICA-0",

                // NORTH-POLE homes
                "NORTH-POLE-NORTH-POLE-0",

                // US homes
                "US-US-0",
                "US-US-1",

                // US replicas
                "EU-US-0",
                "EU-US-1",
                "NORTH-POLE-US-0",
                "NORTH-POLE-US-1",
                "SOUTH-AMERICA-US-0",
                "SOUTH-AMERICA-US-1",
                "ASIA-US-0",
                "ASIA-US-1",

                // EU replicas
                "US-EU-0",
                "US-EU-1",
                "NORTH-POLE-EU-0",
                "NORTH-POLE-EU-1",
                "SOUTH-AMERICA-EU-0",
                "SOUTH-AMERICA-EU-1",
                "ASIA-EU-0",
                "ASIA-EU-1",

                // ASIA replicas
                "US-ASIA-0",
                "NORTH-POLE-ASIA-0",
                "SOUTH-AMERICA-ASIA-0",
                "EU-ASIA-0",

                // SOUTH-AMERICA replicas
                "US-SOUTH-AMERICA-0",
                "NORTH-POLE-SOUTH-AMERICA-0",
                "EU-SOUTH-AMERICA-0",
                "ASIA-SOUTH-AMERICA-0",

                // NORTH-POLE replicas
                "US-NORTH-POLE-0",
                "SOUTH-AMERICA-NORTH-POLE-0",
                "EU-NORTH-POLE-0",
                "ASIA-NORTH-POLE-0",
            };

            expectedSiloRegionsAndServers.Sort();
            var localSiloInfosKeys = localSiloInfos.Keys.ToList<string>();
            localSiloInfosKeys.Sort();
            Xunit.Assert.Equal(expectedSiloRegionsAndServers, localSiloInfosKeys);
        }
    }
}