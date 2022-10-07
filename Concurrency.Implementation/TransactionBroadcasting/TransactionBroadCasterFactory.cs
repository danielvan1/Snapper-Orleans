using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public class TransactionBroadCasterFactory : ITransactionBroadCasterFactory
    {
        private readonly ILogger<TransactionBroadCaster> logger;
        private readonly List<string> regions;

        public TransactionBroadCasterFactory(ILogger<TransactionBroadCaster> logger, List<string> regions)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.regions = regions ?? throw new System.ArgumentNullException(nameof(regions));
        }

        public ITransactionBroadCaster Create(IGrainFactory grainFactory)
        {
            return new TransactionBroadCaster(this.logger, grainFactory, regions);
        }
    }
}