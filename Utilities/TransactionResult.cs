using System;

namespace Utilities
{
    [Serializable]
    public class TransactionResult
    {
        public object resultObj;

        // investigate PACT breakdown latency
        public double prepareTime;    // receive txn request ==> start execute txn
        public double executeTime;    // start execute txn   ==> finish execute txn
        public double commitTime;     // finish execute txn  ==> batch has committed

        public TransactionResult(object resultObj = null)
        {
            this.resultObj = resultObj;
        }
    }
}