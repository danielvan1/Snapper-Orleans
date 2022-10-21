using System;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public record TransactionContext
    {
        // only for PACT
        public long localBid;
        public long localTid;
        public readonly long regionalBid;
        // for global PACT and all ACT
        public readonly long regionalTid;

        public bool IsReplicaTransaction { get; set; } = false;

        /// <summary> This constructor is only for local PACT </summary>
        public TransactionContext(long localTid, long localBid)
        {
            this.localTid = localTid;
            this.localBid = localBid;
            regionalBid = -1;
            regionalTid = -1;
        }

        /// <summary> This constructor is only for global PACT </summary>
        public TransactionContext(long localBid, long localTid, long globalBid, long globalTid)
        {
            this.localBid = localBid;
            this.localTid = localTid;
            this.regionalBid = globalBid;
            this.regionalTid = globalTid;
        }

        public override string ToString()
        {
            return $"regionalBid: {this.regionalBid}, regionalTid: {this.regionalTid}, localBid: {this.localBid}, localTid: {this.localTid}";
        }
    }
}