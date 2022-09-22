using System;
using System.Collections.Generic;
using System.IO;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace SnapperGeoReplication.Tests;

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
}