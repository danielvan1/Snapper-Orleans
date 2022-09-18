using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Configuration
{
    [LocalConfigGrainPlacementStrategy]
    public class LocalConfigGrain : Grain, ILocalConfigGrain
    {
        private readonly ILogger logger;
        private readonly ICoordMap coordMap;
        private readonly LocalConfiguration localConfiguration;
        private int siloID;
        private bool tokenEnabled;

        public LocalConfigGrain(ILogger logger, LocalConfiguration localConfiguration)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.localConfiguration = localConfiguration ?? throw new ArgumentNullException(nameof(localConfiguration));
        }

        public override Task OnActivateAsync()
        {
            this.siloID = (int)this.GetPrimaryKeyLong();
            this.tokenEnabled = false;

            return base.OnActivateAsync();
        }

        public async Task CheckGC()
        {
            Debug.Assert(!Constants.multiSilo || Constants.hierarchicalCoord);
            var tasks = new List<Task>();
            var firstCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToFirstLocalCoordID(siloID);

            for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
            {
                var coordID = i + firstCoordID;
                var coord = this.GrainFactory.GetGrain<ILocalCoordGrain>(coordID);
                tasks.Add(coord.CheckGC());
            }

            await Task.WhenAll(tasks);
        }

        public async Task ConfigLocalEnv()
        {
            Console.WriteLine($"local config grain {siloID} is initiated, silo ID = {siloID}");

            // in this case, all coordinators locate in a separate silo
            this.coordMap.Init(this.GrainFactory);

            // initialize local coordinators in this silo
            var tasks = new List<Task>();
            var firstCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToFirstLocalCoordID(siloID);

            for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
            {
                var coordID = i + firstCoordID;
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(coordID);
                tasks.Add(coord.SpawnLocalCoordGrain());
            }

            await Task.WhenAll(tasks);

            if (!this.tokenEnabled)
            {
                // inject token to the first local coordinator in this silo
                var coord0 = GrainFactory.GetGrain<ILocalCoordGrain>(firstCoordID);
                LocalToken token = new LocalToken();
                await coord0.PassToken(token);
                this.tokenEnabled = true;
            }
        }
    }
}