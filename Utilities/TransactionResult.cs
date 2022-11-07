using System;

namespace Utilities
{
    [Serializable]
    public class TransactionResult
    {
        public object ResultObj;

        // investigate PACT breakdown latency
        public double PrepareTime;    // receive txn request ==> start execute txn
        public double ExecuteTime;    // start execute txn   ==> finish execute txn
        public double CommitTime;     // finish execute txn  ==> batch has committed

        public TransactionResult(object resultObj = null)
        {
            this.ResultObj = resultObj;
        }
    }
}