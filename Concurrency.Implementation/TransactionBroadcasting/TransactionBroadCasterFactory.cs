using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans;

namespace Concurrency.Implementation.TransactionBroadcasting
{
    public class TransactionBroadCasterFactory : ITransactionBroadCasterFactory
    {
        private readonly ILogger<TransactionBroadCaster> logger;
        private readonly IIdHelper idHelper;
        private readonly List<string> regions;

        public TransactionBroadCasterFactory(ILogger<TransactionBroadCaster> logger, IIdHelper idHelper, List<string> regions)
        {
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
            this.idHelper = idHelper ?? throw new System.ArgumentNullException(nameof(idHelper));
            this.regions = regions ?? throw new System.ArgumentNullException(nameof(regions));
        }

        public ITransactionBroadCaster Create(IGrainFactory grainFactory)
        {
            return new TransactionBroadCaster(this.logger, this.idHelper, grainFactory, regions);
        }
    }
}