using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class LocalToken : TokenBase
    {
        // for global info
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<GrainAccessInfo, long> PreviousBidPerGrain { get; }

        // public Dictionary<GrainAccessInfo, long> PreviousBidPerGrain { get; }

        // this info is only used for local coordinators
        // TODO: This might need to be using the key: Tuple<int, string>
        // instead of just the string part. But currently the code
        // in DetTxnProcessor is just using 'serviceId' which is the siloID
        // as the index
        public Dictionary<GrainAccessInfo, long> PreviousRegionalBidPerGrain { get; }   // grainID, the regional bid of the latest emitted local batch
        public long LastEmitGlobalBid { get; set; }

        public LocalToken() : base()
        {
            this.LastEmitGlobalBid = -1;
            this.PreviousBidPerGrain = new Dictionary<GrainAccessInfo, long>();
            this.PreviousRegionalBidPerGrain = new Dictionary<GrainAccessInfo, long>();
        }
    }
}