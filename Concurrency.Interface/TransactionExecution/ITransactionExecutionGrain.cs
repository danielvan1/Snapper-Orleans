using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Models;
using Orleans;
using Utilities;

namespace Concurrency.Interface.TransactionExecution
{
    public interface ITransactionExecutionGrain : IGrainWithIntegerCompoundKey
    {
        // PACT
        // These are the methods that the client calls as a entry point
        // to run its methods in transactions
        Task<TransactionResult> StartTransaction(string startFunc, FunctionInput funcInput, List<GrainAccessInfo> grainAccessInfo);

        Task<TransactionResult> StartReplicaTransaction(string firstFunction, FunctionInput functionInput, List<GrainAccessInfo> grainAccessInfo, TransactionContext transactionContext, long highestCommittedBidFromMaster, DateTime dateTime);

        Task<Tuple<object, DateTime>> ExecuteDeterministicTransaction(FunctionCall call, TransactionContext ctx);

        Task ReceiveBatchSchedule(LocalSubBatch batch);

        Task AckBatchCommit(long bid);
    }
}