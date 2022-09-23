using System;
using System.Collections.Generic;
using System.IO;
using GeoSnapperDeployment.Factories;
using GeoSnapperDeployment.Models;
using Microsoft.Extensions.Configuration;
using Xunit;
using System.Linq;
using Orleans.TestingHost;
using SmallBank.Interfaces;
using Concurrency.Interface.Configuration;
using System.Threading.Tasks;

namespace SnapperGeoIntegration.Tests;

[Collection("Simple Bank Regional Integration Tests")]
public class SimpleBankRegionalIntegrationTest
{
    public SimpleBankRegionalIntegrationTest()
    {
    }

    [Fact]
    public async void TestSimpleRegionalMultiTransferTransaction()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<RegionalIntegrationTestConfiguration>();
        var cluster = builder.Build();
        cluster.Deploy();

        IRegionalConfigGrain regionalConfigGrainEU = cluster.GrainFactory.GetGrain<IRegionalConfigGrain>(0, "EU");
        ILocalConfigGrain localConfigGrainEU = cluster.GrainFactory.GetGrain<ILocalConfigGrain>(3, "EU");

        var task1 = regionalConfigGrainEU.InitializeRegionalCoordinators("EU");
        var task2 = localConfigGrainEU.InitializeLocalCoordinators("EU");
        List<Task> configureAllConfigAndCoordinators = new List<Task>()
        {
            task1, task2
        };

        try {

            await Task.WhenAll(configureAllConfigAndCoordinators);
        } catch (Exception e)
        {
            Console.WriteLine(e.StackTrace);
        }

        Type snapperTransactionalAccountGrainType = typeof(SmallBank.Grains.SnapperTransactionalAccountGrain);
        string snapperTransactionalAccountGrainTypeName = snapperTransactionalAccountGrainType.ToString();

        int actorId0 = 0;
        int actorId1 = 1;
        var deployedRegion = "EU";
        var homeRegion = "EU";
        var server0 = "0";
        var server1 = "1";
        var regionAndServer0 = $"{deployedRegion}-{homeRegion}-{server0}";
        var regionAndServer1 = $"{deployedRegion}-{homeRegion}-{server1}";


        var actorAccessInfo0 = new List<Tuple<int, string>>() 
        {
            new Tuple<int, string>(actorId0, regionAndServer0),
        };

        var actorAccessInfo1 = new List<Tuple<int, string>>() 
        {
            new Tuple<int, string>(actorId1, regionAndServer1),
        };

        var grainClassName = new List<string>();
        grainClassName.Add(snapperTransactionalAccountGrainTypeName);

        var actor0 = cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(actorId0, regionAndServer0);
        var accountId = actorId1;

        var actor1 = cluster.GrainFactory.GetGrain<ISnapperTransactionalAccountGrain>(actorId1, regionAndServer1);

        var grainClassNamesForMultiTransfer = new List<string>();                                             // grainID, grainClassName
        grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);
        grainClassNamesForMultiTransfer.Add(snapperTransactionalAccountGrainTypeName);

        var actorAccessInfoForMultiTransfer = new List<Tuple<int, string>>() 
        {
            new Tuple<int, string>(actorId0, regionAndServer0),
            new Tuple<int, string>(actorId1, regionAndServer1),
        };

        var amountToDeposit = 50;

        try {
            await actor0.StartTransaction("Init", new Tuple<int, string>(actorId0, regionAndServer0), actorAccessInfo0, grainClassName);
            await actor1.StartTransaction("Init", new Tuple<int, string>(actorId1, regionAndServer1), actorAccessInfo1, grainClassName);
            var multiTransferInput = new Tuple<int, List<Tuple<int, string>>>(
                amountToDeposit,
                new List<Tuple<int, string>>() { new Tuple<int, string>(actorId1, regionAndServer1) 
            });  // money, List<to account>
            await actor0.StartTransaction("MultiTransfer", multiTransferInput, actorAccessInfoForMultiTransfer, grainClassNamesForMultiTransfer);
            var PACTBalanceActor0 = await actor0.StartTransaction("Balance", null, actorAccessInfo0, grainClassName);
            Xunit.Assert.Equal(50, Convert.ToInt32(PACTBalanceActor0.resultObj));
            var PACTBalanceActor1 = await actor1.StartTransaction("Balance", null, actorAccessInfo1, grainClassName);
            Xunit.Assert.Equal(150, Convert.ToInt32(PACTBalanceActor1.resultObj));
        } catch (Exception e) {
            Console.WriteLine(e.Message);
            Console.WriteLine(e.StackTrace);
            // TODO: Find a nicer way to just fail the test when it catches an exception
            Xunit.Assert.True(false);
        }

        cluster.StopAllSilos();
    }
}