using System;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public record TransactionContext
    {
        // only for PACT
        public long LocalBid { get; set; }
        public long LocalTid { get; set; }
        public long RegionalBid { get; }
        // for global PACT and all ACT
        public long RegionalTid { get; }

        public bool IsReplicaTransaction { get; set; } = false;
        public double Latency { get; set; }

        /// <summary> This constructor is only for local PACT </summary>
        public TransactionContext(long localTid, long localBid)
        {
            this.LocalTid = localTid;
            this.LocalBid = localBid;
            RegionalBid = -1;
            RegionalTid = -1;
        }

        /// <summary> This constructor is only for global PACT </summary>
        public TransactionContext(long localBid, long localTid, long globalBid, long globalTid)
        {
            this.LocalBid = localBid;
            this.LocalTid = localTid;
            this.RegionalBid = globalBid;
            this.RegionalTid = globalTid;
        }

        public override string ToString()
        {
            return $"regionalBid: {this.RegionalBid}, regionalTid: {this.RegionalTid}, localBid: {this.LocalBid}, localTid: {this.LocalTid}, IsReplication: {this.IsReplicaTransaction}";
        }
    }
}