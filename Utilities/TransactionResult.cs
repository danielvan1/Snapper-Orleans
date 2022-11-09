using System;

namespace Utilities
{
    [Serializable]
    public class TransactionResult
    {
        public object Result { get; init; }

        // investigate PACT breakdown latency
        public double PrepareTime { get; init; }    // receive txn request ==> start execute txn
        public double ExecuteTime { get; init; }    // start execute txn   ==> finish execute txn
        public double CommitTime { get; init; }     // finish execute txn  ==> batch has committed
        public double Latency { get; init; }
        public bool IsReplica { get; init; }
        public string FirstFunctionName { get; init; }
    }
}