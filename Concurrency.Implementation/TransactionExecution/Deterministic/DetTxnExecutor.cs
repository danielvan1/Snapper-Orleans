using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Concurrency.Interface.TransactionExecution;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DetTxnExecutor<TState> where TState : ICloneable, ISerializable
    {
        private readonly ILogger logger;
        private readonly GrainReference grainReference;
        private readonly TransactionExecutionGrainId myId;

        // grain basic info
        private readonly int myID;
        private readonly string siloID;

        // transaction execution
        TransactionScheduler myScheduler;
        ITransactionalState<TState> state;

        // local and global coordinators
        readonly int myLocalCoordID;
        readonly ICoordMap coordMap;
        readonly ILocalCoordinatorGrain myLocalCoord;
        readonly IRegionalCoordinatorGrain myRegionalCoordinator;                                // use this coord to get tid for global transactions
        private readonly IGrainFactory grainFactory;

        // PACT execution
        Dictionary<long, TaskCompletionSource<bool>> localBatchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        Dictionary<long, BasicFuncResult> detFuncResults;                        // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times

        // only for global PACT
        Dictionary<long, long> globalBidToLocalBid;
        Dictionary<long, Dictionary<long, long>> globalTidToLocalTidPerBatch;    // key: global bid, <global tid, local tid>
        Dictionary<long, TaskCompletionSource<bool>> globalBtchInfoPromise;      // key: global bid, use to check if the SubBatch has arrived or not

        public void CheckGC()
        {
            if (localBatchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: localBtchInfoPromise.Count = {localBatchInfoPromise.Count}");
            if (detFuncResults.Count != 0) Console.WriteLine($"DetTxnExecutor: detFuncResults.Count = {detFuncResults.Count}");
            if (globalBidToLocalBid.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBidToLocalBid.Count = {globalBidToLocalBid.Count}");
            if (globalTidToLocalTidPerBatch.Count != 0) Console.WriteLine($"DetTxnExecutor: globalTidToLocalTidPerBatch.Count = {globalTidToLocalTidPerBatch.Count}");
            if (globalBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBtchInfoPromise.Count = {globalBtchInfoPromise.Count}");
        }

        public DetTxnExecutor(
            ILogger logger,
            GrainReference grainReference,
            TransactionExecutionGrainId myId,
            int myID,
            string siloID,
            int myLocalCoordID,
            ILocalCoordinatorGrain myLocalCoord,
            IRegionalCoordinatorGrain myRegionalCoordinator,
            IGrainFactory grainFactory,
            TransactionScheduler myScheduler,
            ITransactionalState<TState> state
            )
        {
            this.logger = logger;
            this.grainReference = grainReference;
            this.myId = myId;
            this.myID = myID;
            this.siloID = siloID;
            this.myLocalCoordID = myLocalCoordID;
            this.myLocalCoord = myLocalCoord;
            this.myRegionalCoordinator = myRegionalCoordinator;
            this.grainFactory = grainFactory;
            this.myScheduler = myScheduler;
            this.state = state;

            localBatchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
            detFuncResults = new Dictionary<long, BasicFuncResult>();
            globalBidToLocalBid = new Dictionary<long, long>();
            globalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            globalBtchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // int: the highestCommittedBid get from local coordinator
        public async Task<Tuple<long, TransactionContext>> GetDetContext(List<Tuple<int, string>> grainList, List<string> grainClassName)
        {
            if (Constants.multiSilo && Constants.hierarchicalCoord)
            {
                // check if the transaction will access multiple silos
                var siloList = new List<Tuple<int, string>>();
                var grainListPerSilo = new Dictionary<string, List<Tuple<int, string>>>();
                var grainNamePerSilo = new Dictionary<string, List<string>>();

                // This is the placement manager(PM) code described in the paper
                for (int i = 0; i < grainList.Count; i++)
                {
                    var grainID = grainList[i];
                    if (grainListPerSilo.ContainsKey(grainID.Item2) == false)
                    {
                        siloList.Add(grainID);
                        grainListPerSilo.Add(grainID.Item2, new List<Tuple<int, string>>());
                        grainNamePerSilo.Add(grainID.Item2, new List<string>());
                    }

                    grainListPerSilo[grainID.Item2].Add(grainID);
                    grainNamePerSilo[grainID.Item2].Add(grainClassName[i]);
                }

                // For a simple example, make sure that only 1 silo is involved in the transaction
                this.logger.LogInformation($"Silolist count: {siloList.Count}", this.grainReference);
                if (siloList.Count != 1)
                {
                    this.logger.LogError("Should not go this path", this.grainReference);

                    // get regional tid from regional coordinator
                    // TODO: Should be our Regional Coordinators here.
                    // Note the Dictionary<string, Tuple<int, string>> part of the
                    // return type of NewTransaction(..) is a map between the region
                    // and which local coordinators
                    Tuple<TransactionRegistInfo, Dictionary<Tuple<int, string>, Tuple<int, string>>> regionalInfo = await myRegionalCoordinator.NewTransaction(siloList);
                    var regionalTid = regionalInfo.Item1.tid;
                    var regionalBid = regionalInfo.Item1.bid;
                    var siloIDToLocalCoordID = regionalInfo.Item2;

                    // send corresponding grainAccessInfo to local coordinators in different silos
                    Debug.Assert(grainListPerSilo.ContainsKey(siloID));
                    Task<TransactionRegistInfo> task = null;
                    for (int i = 0; i < siloList.Count; i++)
                    {
                        var siloID = siloList[i];
                        Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloID));

                        // TODO: Need a map from coordinator to local coordinator
                        var coordID = siloIDToLocalCoordID[siloID];
                        var localCoordinator = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordID.Item1, coordID.Item2);

                        // get local tid, bid from local coordinator
                        if (coordID.Item2 == this.siloID)
                        {
                            this.logger.Info($"[region:{this.siloID};id:{this.myID}] Is calling NewGlobalTransaction w/ task");
                            task = localCoordinator.NewGlobalTransaction(regionalBid, regionalTid, grainListPerSilo[siloID.Item2], grainNamePerSilo[siloID.Item2]);
                        }
                        else
                        {
                            this.logger.Info($"[region:{this.siloID};id:{this.myID}] Is calling NewGlobalTransaction w/o task");
                            _ = localCoordinator.NewGlobalTransaction(regionalBid, regionalTid, grainListPerSilo[siloID.Item2], grainNamePerSilo[siloID.Item2]);
                        }
                    }


                    Debug.Assert(task != null);
                    this.logger.Info($"[region:{this.siloID};id:{this.myID}] Is just waiting for task in GetDetContext");
                    TransactionRegistInfo localInfo = await task;
                    this.logger.Info($"[region:{this.siloID};id:{this.myID}] Is DONE waiting for task in GetDetContext, going to return tx context");
                    var cxt1 = new TransactionContext(localInfo.bid, localInfo.tid, regionalBid, regionalTid);

                    // TODO: What is this -1??
                    return new Tuple<long, TransactionContext>(-1, cxt1) ;
                }
            }

            this.logger.LogInformation($"GetDetContext going to call myLocalCoord.NewTransaction", this.grainReference);
            var info = await myLocalCoord.NewTransaction(grainList, grainClassName);
            this.logger.LogInformation($"GetDetContext after call to myLocalCoord.NewTransaction", this.grainReference);
            var cxt2 = new TransactionContext(info.tid, info.bid);

            return new Tuple<long, TransactionContext>(info.highestCommittedBid, cxt2);
        }

        public async Task WaitForTurn(TransactionContext cxt)
        {
            // check if it is a global PACT
            if (cxt.globalBid != -1)
            {
                // wait until the SubBatch has arrived this grain
                if (globalBtchInfoPromise.ContainsKey(cxt.globalBid) == false)
                    globalBtchInfoPromise.Add(cxt.globalBid, new TaskCompletionSource<bool>());
                await globalBtchInfoPromise[cxt.globalBid].Task;

                // need to map global info to the corresponding local tid and bid
                cxt.localBid = globalBidToLocalBid[cxt.globalBid];
                cxt.localTid = globalTidToLocalTidPerBatch[cxt.globalBid][cxt.globalTid];
            }
            else
            {
                this.logger.LogInformation("WaitForturn waiting", this.grainReference);
                // wait until the SubBatch has arrived this grain
                if (localBatchInfoPromise.ContainsKey(cxt.localBid) == false)
                    localBatchInfoPromise.Add(cxt.localBid, new TaskCompletionSource<bool>());
                await localBatchInfoPromise[cxt.localBid].Task;

                this.logger.LogInformation("WaitForturn finished", this.grainReference);
            }

            Debug.Assert(detFuncResults.ContainsKey(cxt.localTid) == false);
            detFuncResults.Add(cxt.localTid, new BasicFuncResult());
            await myScheduler.WaitForTurn(cxt.localBid, cxt.localTid);
        }

        public async Task FinishExecuteDetTxn(TransactionContext cxt)
        {
            var coordId = myScheduler.AckComplete(cxt.localBid, cxt.localTid);
            if (coordId != -1)   // the current batch has completed on this grain
            {
                localBatchInfoPromise.Remove(cxt.localBid);
                if (cxt.globalBid != -1)
                {
                    globalBidToLocalBid.Remove(cxt.globalBid);
                    globalTidToLocalTidPerBatch.Remove(cxt.globalBid);
                    globalBtchInfoPromise.Remove(cxt.globalBid);
                }

                myScheduler.scheduleInfo.CompleteDetBatch(cxt.localBid);

                var localCoordinatorId = this.myId.IntId % Constants.NumberOfLocalCoordinatorsPerSilo;
                var localCoordinatorRegion = this.myId.StringId;
                // TODO: This coordinator should be the one that sent the batch
                var coord = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(coordId, localCoordinatorRegion);
                this.logger.LogInformation($"Send the local coordinator(int id: {localCoordinatorId}, region:{localCoordinatorRegion}) the acknowledgement of the batch commit for batch id:{cxt.localBid}", this.grainReference);
                _ = coord.AckBatchCompletion(cxt.localBid);
            }
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public void BatchArrive(LocalSubBatch batch)
        {
            this.logger.LogInformation($"Batch arrived, batch: {batch}", this.grainReference);
            if (localBatchInfoPromise.ContainsKey(batch.bid) == false)
                localBatchInfoPromise.Add(batch.bid, new TaskCompletionSource<bool>());
            this.logger.LogInformation($"In BatchArrive: localBtchInfoPromise[batch.bid]: {localBatchInfoPromise[batch.bid]}", this.grainReference);
            localBatchInfoPromise[batch.bid].SetResult(true);

            // register global info mapping if necessary
            if (batch.globalBid != -1)
            {
                globalBidToLocalBid.Add(batch.globalBid, batch.bid);
                globalTidToLocalTidPerBatch.Add(batch.globalBid, batch.globalTidToLocalTid);

                if (globalBtchInfoPromise.ContainsKey(batch.globalBid) == false)
                    globalBtchInfoPromise.Add(batch.globalBid, new TaskCompletionSource<bool>());
                globalBtchInfoPromise[batch.globalBid].SetResult(true);
            }
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public TState GetState(long tid, AccessMode mode)
        {
            if (mode == AccessMode.Read)
            {
                detFuncResults[tid].isNoOpOnGrain = false;
                detFuncResults[tid].isReadOnlyOnGrain = true;
            }
            else
            {
                detFuncResults[tid].isNoOpOnGrain = false;
                detFuncResults[tid].isReadOnlyOnGrain = false;
            }
            return state.DetOp();
        }

        public async Task<TransactionResult> CallGrain(TransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            this.logger.LogInformation("Inside CallGrain, going to call (await grain.ExecuteDet(call, cxt))", this.grainReference);
            var resultObj = (await grain.ExecuteDet(call, cxt)).Item1;
            this.logger.LogInformation("Inside CallGrain, after call to (await grain.ExecuteDet(call, cxt))", this.grainReference);
            return new TransactionResult(resultObj);
        }

        public void CleanUp(long tid)
        {
            detFuncResults.Remove(tid);
        }
    }
}
