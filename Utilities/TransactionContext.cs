using System;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public class TransactionContext
    {
        // only for PACT
        public long localBid;
        public long localTid;
        public readonly long globalBid;

        // for global PACT and all ACT
        public readonly long globalTid;


        /// <summary> This constructor is only for local PACT </summary>
        public TransactionContext(long localTid, long localBid)
        {
            this.localTid = localTid;
            this.localBid = localBid;
            globalBid = -1;
            globalTid = -1;
        }

        /// <summary> This constructor is only for global PACT </summary>
        public TransactionContext(long localBid, long localTid, long globalBid, long globalTid)
        {
            this.localBid = localBid;
            this.localTid = localTid;
            this.globalBid = globalBid;
            this.globalTid = globalTid;
        }
    }
}