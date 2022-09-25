using System;

namespace Utilities
{
    [Serializable]
    public record TransactionRegisterInfo
    {
        public long Tid { get; init; }
        public long Bid { get; init; }
        public long HighestCommittedBid { get; init; }

        /// <summary> This constructor is only for PACT </summary>
        public TransactionRegisterInfo(long bid, long tid, long highestCommittedBid)
        {
            this.Tid = tid;
            this.Bid = bid;
            this.HighestCommittedBid = highestCommittedBid;
        }

        public override string ToString()
        {
            return $"tid: {this.Tid}, bid: {this.Bid}, highestCommittedBid: {this.HighestCommittedBid}";
        }
    }
}