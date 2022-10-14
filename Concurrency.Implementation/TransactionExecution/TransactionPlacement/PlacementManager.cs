using System;
using System.Collections.Generic;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;

namespace Concurrency.Implementation.TransactionExecution.TransactionPlacement
{
    public class PlacementManager : IPlacementManager
    {
        private readonly ILogger<PlacementManager> logger;

        public PlacementManager(ILogger<PlacementManager> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public TransactionType GetTransactionType(List<GrainAccessInfo> grainAccessInfos)
        {
            // Check if the transaction will access multiple silos
            HashSet<string> uniqueSiloIds = this.GetUniqueSiloIds(grainAccessInfos);

            return uniqueSiloIds.Count > 1 ? TransactionType.SingleHomeMultiServer : TransactionType.SingleHomeSingleServer;
        }

        private HashSet<string> GetUniqueSiloIds(List<GrainAccessInfo> grainAccessInfos)
        {
            // Check if the transaction will access multiple silos
            var siloIds = new HashSet<string>();

            // This is the placement manager(PM) code described in the paper
            foreach (GrainAccessInfo grainAccessInfo in grainAccessInfos)
            {
                var siloId = grainAccessInfo.Region;

                this.logger.LogInformation("SiloId: {siloId}", siloId);

                if (!siloIds.Contains(siloId))
                {
                    siloIds.Add(siloId);
                }
            }

            return siloIds;
        }

        private bool IsMultiHome(IEnumerable<string> siloIds)
        {
            bool IsMultiHome = false;

            return IsMultiHome;
        }
    }
}