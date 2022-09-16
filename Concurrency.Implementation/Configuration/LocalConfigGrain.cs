using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Implementation.Configuration
{
    [LocalConfigGrainPlacementStrategy]
    public class LocalConfigGrain : Grain, ILocalConfigGrain
    {
        private readonly ILoggerGroup loggerGroup;  // this logger group is only accessible within this silo host
        private readonly ICoordMap coordMap;
        private readonly LocalConfiguration localConfiguration;
        private int siloID;
        private bool tokenEnabled;

        public LocalConfigGrain(LocalConfiguration localConfiguration)
        {
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
            if (Constants.loggingType == LoggingType.LOGGER) this.loggerGroup.Init(Constants.numLoggerPerSilo, $"Silo{siloID}_LocalLog");

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