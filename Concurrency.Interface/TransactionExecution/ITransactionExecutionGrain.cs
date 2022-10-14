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
        Task<TransactionResult> StartTransaction(string startFunc, FunctionInput funcInput, List<Tuple<int, string>> grainAccessInfo, List<string> grainClassName);

        Task<Tuple<object, DateTime>> ExecuteDeterministicTransaction(FunctionCall call, TransactionContext ctx);

        Task ReceiveBatchSchedule(LocalSubBatch batch);

        Task AckBatchCommit(long bid);
    }
}