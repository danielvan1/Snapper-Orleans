using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class LocalSubBatch : SubBatch     // sent from local coordinator to transactional grains
    {
        public readonly long globalBid;
        public Dictionary<long, long> globalTidToLocalTid;

        public long highestCommittedBid;

        public LocalSubBatch(long globalBid, SubBatch subBatch) : base(subBatch)
        {
            this.globalBid = globalBid;
            globalTidToLocalTid = new Dictionary<long, long>();
            highestCommittedBid = -1;
        }
    }
}