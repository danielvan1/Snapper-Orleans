using System;
using System.Collections.Generic;
using System.Linq;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class BasicToken
    {
        public long previousEmitBid;
        public long previousEmitTid;
        public long previousCoordID;
        public long highestCommittedBid;
        public bool isLastEmitBidGlobal;
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<Tuple<int, string>, long> previousBidPerService;

        // this info is only used for local coordinators
        // TODO: This might need to be using the key: Tuple<int, string>
        // instead of just the string part. But currently the code
        // in DetTxnProcessor is just using 'serviceId' which is the siloID
        // as the index
        public Dictionary<Tuple<int, string>, long> previousRegionalBidPerGrain;   // grainID, the regional bid of the latest emitted local batch

        public BasicToken()
        {
            this.previousEmitBid = -1;
            this.previousEmitTid = -1;
            this.previousCoordID = -1;
            this.highestCommittedBid = -1;
            this.isLastEmitBidGlobal = false;
            this.previousBidPerService = new Dictionary<Tuple<int, string>, long>();
            this.previousRegionalBidPerGrain = new Dictionary<Tuple<int, string>, long>();
        }

        public override string ToString()
        {
            return $"lastEmitBid: {this.previousEmitBid}, lastEmitTid: {this.previousEmitTid}, lastCoordId: {this.previousCoordID}, highestCommittedBid: {highestCommittedBid}, isLastEmitBidGlobal: {this.isLastEmitBidGlobal}, previousBidPerService: [{string.Join(", ", this.previousBidPerService.Select(kv => kv.Key + " : " + kv.Value))}]";
        }
    }
}