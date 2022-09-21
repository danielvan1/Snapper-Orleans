using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Threading.Tasks;
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
        private readonly TransactionExecutionGrainId myId;

        // grain basic info
        readonly int myID;
        readonly int siloID;

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
        Dictionary<long, TaskCompletionSource<bool>> localBtchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        Dictionary<long, BasicFuncResult> detFuncResults;                        // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times

        // only for global PACT
        Dictionary<long, long> globalBidToLocalBid;
        Dictionary<long, Dictionary<long, long>> globalTidToLocalTidPerBatch;    // key: global bid, <global tid, local tid>
        Dictionary<long, TaskCompletionSource<bool>> globalBtchInfoPromise;      // key: global bid, use to check if the SubBatch has arrived or not

        public void CheckGC()
        {
            if (localBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: localBtchInfoPromise.Count = {localBtchInfoPromise.Count}");
            if (detFuncResults.Count != 0) Console.WriteLine($"DetTxnExecutor: detFuncResults.Count = {detFuncResults.Count}");
            if (globalBidToLocalBid.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBidToLocalBid.Count = {globalBidToLocalBid.Count}");
            if (globalTidToLocalTidPerBatch.Count != 0) Console.WriteLine($"DetTxnExecutor: globalTidToLocalTidPerBatch.Count = {globalTidToLocalTidPerBatch.Count}");
            if (globalBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBtchInfoPromise.Count = {globalBtchInfoPromise.Count}");
        }

        public DetTxnExecutor(
            ILogger logger,
            TransactionExecutionGrainId myId,
            int myID,
            int siloID,
            int myLocalCoordID,
            ILocalCoordinatorGrain myLocalCoord,
            IRegionalCoordinatorGrain myRegionalCoordinator,
            IGrainFactory grainFactory,
            TransactionScheduler myScheduler,
            ITransactionalState<TState> state
            )
        {
            this.logger = logger;
            this.myId = myId;
            this.myID = myID;
            this.siloID = siloID;
            this.myLocalCoordID = myLocalCoordID;
            this.myLocalCoord = myLocalCoord;
            this.myRegionalCoordinator = myRegionalCoordinator;
            this.grainFactory = grainFactory;
            this.myScheduler = myScheduler;
            this.state = state;

            localBtchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
            detFuncResults = new Dictionary<long, BasicFuncResult>();
            globalBidToLocalBid = new Dictionary<long, long>();
            globalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            globalBtchInfoPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // int: the highestCommittedBid get from local coordinator
        public async Task<Tuple<long, TransactionContext>> GetDetContext(List<int> grainList, List<string> grainClassName)
        {
            if (Constants.multiSilo && Constants.hierarchicalCoord)
            {
                // check if the transaction will access multiple silos
                var siloList = new List<int>();
                var grainListPerSilo = new Dictionary<int, List<int>>();
                var grainNamePerSilo = new Dictionary<int, List<string>>();

                for (int i = 0; i < grainList.Count; i++)
                {
                    var grainID = grainList[i];
                    var siloID = TransactionExecutionGrainPlacementHelper.MapGrainIDToSilo(grainID);
                    if (grainListPerSilo.ContainsKey(siloID) == false)
                    {
                        siloList.Add(siloID);
                        grainListPerSilo.Add(siloID, new List<int>());
                        grainNamePerSilo.Add(siloID, new List<string>());
                    }

                    grainListPerSilo[siloID].Add(grainID);
                    grainNamePerSilo[siloID].Add(grainClassName[i]);
                }

                // For a simple example, make sure that only 1 silo is involved in the transaction
                this.logger.Info($"Silolist count: {siloList.Count}");
                if (siloList.Count != 1)
                {
                    // get global tid from global coordinator
                    // TODO: Should be our Regional Coordinators here.
                    Tuple<TransactionRegistInfo, Dictionary<int, int>> globalInfo = await myRegionalCoordinator.NewTransaction(siloList);
                    var globalTid = globalInfo.Item1.tid;
                    var globalBid = globalInfo.Item1.bid;
                    var siloIDToLocalCoordID = globalInfo.Item2;

                    // send corresponding grainAccessInfo to local coordinators in different silos
                    Debug.Assert(grainListPerSilo.ContainsKey(siloID));
                    Task<TransactionRegistInfo> task = null;
                    for (int i = 0; i < siloList.Count; i++)
                    {
                        var siloID = siloList[i];
                        Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloID));
                        var coordID = siloIDToLocalCoordID[siloID];

                        // get local tid, bid from local coordinator
                        var localCoord = coordMap.GetLocalCoord(coordID);
                        if (siloID == this.siloID)
                        {
                            task = localCoord.NewGlobalTransaction(globalBid, globalTid,
                                                                   grainListPerSilo[siloID], grainNamePerSilo[siloID]);
                        }
                        else
                        {
                            _ = localCoord.NewGlobalTransaction(globalBid, globalTid,
                                                                grainListPerSilo[siloID], grainNamePerSilo[siloID]);
                        }
                    }

                    Debug.Assert(task != null);
                    var localInfo = await task;
                    var cxt1 = new TransactionContext(localInfo.bid, localInfo.tid, globalBid, globalTid);

                    // TODO: What is this -1??
                    return new Tuple<long, TransactionContext>(-1, cxt1) ;
                }
            }

            this.logger.Info($"GetDetContext going to call myLocalCoord.NewTransaction");
            var info = await myLocalCoord.NewTransaction(grainList, grainClassName);
            this.logger.Info($"GetDetContext after call to myLocalCoord.NewTransaction");
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
                this.logger.Info("DetTxExecutor:WaitForturn waiting");
                // wait until the SubBatch has arrived this grain
                if (localBtchInfoPromise.ContainsKey(cxt.localBid) == false)
                    localBtchInfoPromise.Add(cxt.localBid, new TaskCompletionSource<bool>());
                await localBtchInfoPromise[cxt.localBid].Task;

                this.logger.Info("DetTxExecutor:WaitForturn finished");
            }

            Debug.Assert(detFuncResults.ContainsKey(cxt.localTid) == false);
            detFuncResults.Add(cxt.localTid, new BasicFuncResult());
            await myScheduler.WaitForTurn(cxt.localBid, cxt.localTid);
        }

        public async Task FinishExecuteDetTxn(TransactionContext cxt)
        {
            var coordID = myScheduler.AckComplete(cxt.localBid, cxt.localTid);
            if (coordID != -1)   // the current batch has completed on this grain
            {
                localBtchInfoPromise.Remove(cxt.localBid);
                if (cxt.globalBid != -1)
                {
                    globalBidToLocalBid.Remove(cxt.globalBid);
                    globalTidToLocalTidPerBatch.Remove(cxt.globalBid);
                    globalBtchInfoPromise.Remove(cxt.globalBid);
                }

                myScheduler.scheduleInfo.CompleteDetBatch(cxt.localBid);

                var localCoordinatorId = this.myId.IntId % Constants.NumberOfLocalCoordinatorsPerSilo;
                var localCoordinatorRegion = this.myId.StringId;
                var coord = this.grainFactory.GetGrain<ILocalCoordinatorGrain>(localCoordinatorId, localCoordinatorRegion);
                this.logger.Info($"Send the local coordinator(int id: {localCoordinatorId}, region:{localCoordinatorRegion}) the acknowledgement of the batch commit for batch id:{cxt.localBid}");
                _ = coord.AckBatchCompletion(cxt.localBid);
            }
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public void BatchArrive(LocalSubBatch batch)
        {
            this.logger.Info($"Batch arrived, batch: {batch}");
            if (localBtchInfoPromise.ContainsKey(batch.bid) == false)
                localBtchInfoPromise.Add(batch.bid, new TaskCompletionSource<bool>());
            this.logger.Info($"In BatchArrive: localBtchInfoPromise[batch.bid]: {localBtchInfoPromise[batch.bid]}");
            localBtchInfoPromise[batch.bid].SetResult(true);

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
            this.logger.Info("Inside CallGrain, going to call (await grain.ExecuteDet(call, cxt))");
            var resultObj = (await grain.ExecuteDet(call, cxt)).Item1;
            this.logger.Info("Inside CallGrain, after call to (await grain.ExecuteDet(call, cxt))");
            return new TransactionResult(resultObj);
        }

        public void CleanUp(long tid)
        {
            detFuncResults.Remove(tid);
        }
    }
}