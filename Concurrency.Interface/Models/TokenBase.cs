using System;
using System.Collections.Generic;
using System.Linq;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class TokenBase
    {
        public long PreviousEmitBid { get; set; }
        public long PreviousEmitTid { get; set; }
        public long PreviousCoordinatorId { get; set; }
        public long HighestCommittedBid { get; set; }
        public bool IsLastEmitBidGlobal { get; set; }

        public TokenBase()
        {
            this.PreviousEmitBid = -1;
            this.PreviousEmitTid = -1;
            this.PreviousCoordinatorId = -1;
            this.HighestCommittedBid = -1;
            this.IsLastEmitBidGlobal = false;
        }

        public override string ToString()
        {
            return $"lastEmitBid: {this.PreviousEmitBid}, lastEmitTid: {this.PreviousEmitTid}, lastCoordId: {this.PreviousCoordinatorId}, highestCommittedBid: {HighestCommittedBid}, isLastEmitBidGlobal: {this.IsLastEmitBidGlobal}";
        }
    }
}