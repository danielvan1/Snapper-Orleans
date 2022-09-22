using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class SubBatch   // sent from global coordinator to local coordinator
    {
        public readonly long bid;
        public readonly long coordID;

        public long lastBid;
        public List<long> txnList;

        // NOTICE: this field is only used for batches generated by local coordinators
        public long lastGlobalBid;

        public SubBatch(long bid, long coordID)
        {
            lastBid = -1;
            this.bid = bid;
            this.coordID = coordID;
            txnList = new List<long>();
            lastGlobalBid = -1;
        }

        public SubBatch(SubBatch subBatch)
        {
            bid = subBatch.bid;
            coordID = subBatch.coordID;
            lastBid = subBatch.lastBid;
            txnList = subBatch.txnList;
            lastGlobalBid = subBatch.lastGlobalBid;
        }
    }
}