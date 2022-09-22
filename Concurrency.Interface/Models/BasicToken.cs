using System;
using System.Collections.Generic;

namespace Concurrency.Interface.Models
{
    [Serializable]
    public class BasicToken
    {
        public long lastEmitBid;
        public long lastEmitTid;
        public long lastCoordID;
        public long highestCommittedBid;
        public bool isLastEmitBidGlobal;
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<Tuple<int, string>, long> lastBidPerService;

        // this info is only used for local coordinators
        // TODO: This might need to be using the key: Tuple<int, string>
        // instead of just the string part. But currently the code
        // in DetTxnProcessor is just using 'serviceId' which is the siloID
        // as the index
        public Dictionary<Tuple<int, string>, long> lastGlobalBidPerGrain;   // grainID, the global bid of the latest emitted local batch

        public BasicToken()
        {
            lastEmitBid = -1;
            lastEmitTid = -1;
            lastCoordID = -1;
            highestCommittedBid = -1;
            isLastEmitBidGlobal = false;
            lastBidPerService = new Dictionary<Tuple<int, string>, long>();
            lastGlobalBidPerGrain = new Dictionary<Tuple<int, string>, long>();
        }
    }
}