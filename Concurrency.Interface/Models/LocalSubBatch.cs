using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public record LocalSubBatch : SubBatch     // sent from local coordinator to transactional grains
    {
        public long RegionalBid { get; init; }

        /// <summary>
        /// This dictionary is used for context switching.
        /// Currently we are leaking the reference of this dictionary in the DetTxnExecutor and using the reference over there
        /// to ensure the globalTids corresponds to the localTids
        /// </summary>
        public Dictionary<long, long> RegionalTidToLocalTid { get; init; } = new Dictionary<long, long>();

        public long HighestCommittedBid { get; init; } = -1;

        public LocalSubBatch(SubBatch subBatch) : base(subBatch) { }

        public override string ToString()
        {
            return $"GlobalBid: {this.RegionalBid}, HighestCommittedBid: {this.HighestCommittedBid}, {base.ToString()}";
        }
    }
}