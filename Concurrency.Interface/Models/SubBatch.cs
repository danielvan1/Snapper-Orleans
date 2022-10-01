using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    //TODO: Build Compare to be able to sort on BID
    [Serializable]
    public record SubBatch   // sent from global coordinator to local coordinator
    {
        public long Bid { get; init; }

        public long CoordinatorId { get; init; }

        public long PreviousBid { get; set; }

        public List<long> Transactions { get; init; }

        // NOTICE: this field is only used for batches generated by local coordinators
        public long previousGlobalBid;

        public SubBatch(long bid, long coordinatorId)
        {
            this.PreviousBid = -1;
            this.Bid = bid;
            this.CoordinatorId = coordinatorId;
            this.Transactions = new List<long>();
            this.previousGlobalBid = -1;
        }

        public SubBatch(SubBatch subBatch)
        {
            this.Bid = subBatch.Bid;
            this.CoordinatorId = subBatch.CoordinatorId;
            this.PreviousBid = subBatch.PreviousBid;
            this.Transactions = subBatch.Transactions;
            this.previousGlobalBid = subBatch.previousGlobalBid;
        }

        public override string ToString()
        {
            return $"Bid: {this.Bid}, PreviousBid: {this.PreviousBid}, CoordinatorId: {this.CoordinatorId}, lastGlobalBid: {previousGlobalBid}, Transactions: [{string.Join(", ", this.Transactions)}]";
        }
    }
}