﻿using Concurrency.Utilities;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface
{
    public interface ITransactionExecutionGrain : Orleans.IGrainWithIntegerKey, Orleans.IGrainWithGuidKey
    {

        /*
         * Client calls this function to submit a determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<FunctionResult> StartTransaction(Dictionary<Guid, Tuple<String,int>> grainAccessInformation, String startFunction, FunctionInput inputs);

        /*  
         * Client calls this function to submit a non-determinictic transaction to the transaction coordinator.
         */
        [AlwaysInterleave]
        Task<FunctionResult> StartTransaction(String startFunction, FunctionInput inputs);

        /*
         * Receive batch schedule from the coordinator.
         */
        [AlwaysInterleave]
        Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule);

        /*
         * Called by other grains to execute a function.
         */
        [AlwaysInterleave]
        Task<FunctionResult> Execute(FunctionCall call);

        [AlwaysInterleave]
        Task<bool> Prepare(long tid);

        [AlwaysInterleave]
        Task Commit(long tid);

        [AlwaysInterleave]
        Task Abort(long tid);

    }
}
