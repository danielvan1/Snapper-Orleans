using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record LocalSubBatch : SubBatch     // sent from local coordinator to transactional grains
    {
        public long GlobalBid { get; init; }

        public Dictionary<long, long> GlobalTidToLocalTid { get; set; }

        public long HighestCommittedBid { get; set; }

        public LocalSubBatch(long globalBid, SubBatch subBatch) : base(subBatch)
        {
            this.GlobalBid = globalBid;
            this.GlobalTidToLocalTid = new Dictionary<long, long>();
            this.HighestCommittedBid = -1;
        }
    }
}