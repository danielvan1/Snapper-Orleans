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
        public long PreviousCoordID { get; set; }
        public long HighestCommittedBid { get; set; }
        public bool IsLastEmitBidGlobal { get; set; }
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<Tuple<int, string>, long> previousBidPerService { get; }

        // this info is only used for local coordinators
        // TODO: This might need to be using the key: Tuple<int, string>
        // instead of just the string part. But currently the code
        // in DetTxnProcessor is just using 'serviceId' which is the siloID
        // as the index
        public Dictionary<Tuple<int, string>, long> previousRegionalBidPerGrain { get; }   // grainID, the regional bid of the latest emitted local batch

        public TokenBase()
        {
            this.PreviousEmitBid = -1;
            this.PreviousEmitTid = -1;
            this.PreviousCoordID = -1;
            this.HighestCommittedBid = -1;
            this.IsLastEmitBidGlobal = false;
            this.previousBidPerService = new Dictionary<Tuple<int, string>, long>();
            this.previousRegionalBidPerGrain = new Dictionary<Tuple<int, string>, long>();
        }

        public override string ToString()
        {
            return $"lastEmitBid: {this.PreviousEmitBid}, lastEmitTid: {this.PreviousEmitTid}, lastCoordId: {this.PreviousCoordID}, highestCommittedBid: {HighestCommittedBid}, isLastEmitBidGlobal: {this.IsLastEmitBidGlobal}, previousBidPerService: [{string.Join(", ", this.previousBidPerService.Select(kv => kv.Key + " : " + kv.Value))}]";
        }
    }
}