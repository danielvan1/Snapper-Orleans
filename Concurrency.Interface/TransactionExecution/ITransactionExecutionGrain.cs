using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using Concurrency.Interface.Models;
using Orleans;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionExecutionGrain : IGrainWithIntegerCompoundKey
    {
        // PACT
        // These are the methods that the client calls as a entry point
        // to run its methods in transactions
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName);
        Task<Tuple<object, DateTime>> ExecuteDet(FunctionCall call, TransactionContext ctx);
        Task ReceiveBatchSchedule(LocalSubBatch batch);
        Task AckBatchCommit(long bid);


        // ACT
        // We don't support ACT right now in replicated Snapper
        Task<Tuple<NonDetFuncResult, DateTime>> ExecuteNonDet(FunctionCall call, TransactionContext ctx);
        Task<bool> Prepare(long tid, bool isReader);
        Task Commit(long tid, long maxBeforeLocalBid, long maxBeforeGlobalBid);
        Task Abort(long tid);

        // hybrid execution
        Task WaitForBatchCommit(long bid);

        Task CheckGC();
    }
}