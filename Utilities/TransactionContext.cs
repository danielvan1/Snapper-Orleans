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

        public long HighestCommittedBid { get; init; }
        public bool IsReplicaTransaction { get; set; } = false;

        public TransactionContext(long localTid, long localBid)
        {
            this.LocalTid = localTid;
            this.LocalBid = localBid;
            this.RegionalBid = -1;
            this.RegionalTid = -1;
        }

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