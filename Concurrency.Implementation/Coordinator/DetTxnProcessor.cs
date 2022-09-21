using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class DetTxnProcessor
    {
        private readonly ILogger logger;
        private readonly Random random;
        readonly int myID;
        private readonly bool isRegionalCoordinator;
        public long highestCommittedBid;
        readonly ICoordMap coordMap;

        // transaction processing
        private IList<List<Tuple<int, string>>> deterministicRequests;
        List<TaskCompletionSource<Tuple<long, long>>> detRequestPromise; // <local bid, local tid>

        // batch processing
        Dictionary<long, long> bidToLastBid;
        Dictionary<long, int> bidToLastCoordID; // <bid, coordID who emit this bid's lastBid>
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches; // <bid, Service ID, subBatch>
        Dictionary<long, TaskCompletionSource<bool>> batchCommit;
        // only for global batch
        Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch; // global bid, silo ID, chosen local coord ID

        public DetTxnProcessor(
            ILogger logger,
            int myID,
            Dictionary<long, int> expectedAcksPerBatch,
            Dictionary<long, Dictionary<Tuple<int, string>, SubBatch>> bidToSubBatches,
            Dictionary<long, Dictionary<Tuple<int, string>, Tuple<int, string>>> localCoordinatorPerSiloPerBatch = null)
        {
            this.logger = logger;
            this.random = new Random();
            this.myID = myID;
            this.coordMap = coordMap;
            bidToLastBid = new Dictionary<long, long>();
            bidToLastCoordID = new Dictionary<long, int>();
            this.expectedAcksPerBatch = expectedAcksPerBatch;
            this.bidToSubBatches = bidToSubBatches;

            // TODO: Consider if this following two lines are equivalent
            // to the previous code, I think it is. 
            this.isRegionalCoordinator = localCoordinatorPerSiloPerBatch != null;
            this.localCoordinatorPerSiloPerBatch = localCoordinatorPerSiloPerBatch;

            Init();
        }

        public void CheckGC()
        {
            if (this.deterministicRequests.Count != 0) Console.WriteLine($"DetTxnProcessor: detRequests.Count = {this.deterministicRequests.Count}");
            if (this.detRequestPromise.Count != 0) Console.WriteLine($"DetTxnProcessor: detRequestPromise.Count = {detRequestPromise.Count}");
            if (this.batchCommit.Count != 0) Console.WriteLine($"DetTxnProcessor: batchCommit.Count = {batchCommit.Count}");
            if (this.bidToLastCoordID.Count != 0) Console.WriteLine($"DetTxnProcessor {myID}: bidToLastCoordID.Count = {bidToLastCoordID.Count}");
            if (this.bidToLastBid.Count != 0) Console.WriteLine($"DetTxnProcessor {myID}: bidToLastBid.Count = {bidToLastBid.Count}");
        }

        public void Init()
        {
            this.highestCommittedBid = -1;
            this.deterministicRequests = new List<List<Tuple<int, string>>>();
            this.detRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            this.batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // for PACT
        public async Task<Tuple<long, long>> NewDet(List<Tuple<int, string>> serviceList)   // returns a Tuple<bid, tid>
        {
            this.deterministicRequests.Add(serviceList);
            var promise = new TaskCompletionSource<Tuple<long, long>>();
            this.detRequestPromise.Add(promise);
            this.logger.LogInformation("Waiting in NewDet");
            // Waits for this.GenerateBatch(..) to give it a batchId and transactionId
            await promise.Task;
            this.logger.LogInformation("finished NewDet");
            return new Tuple<long, long>(promise.Task.Result.Item1, promise.Task.Result.Item2);
        }

        public long GenerateBatch(BasicToken token)
        {
            if (this.deterministicRequests.Count == 0)
            {
                return -1;
            }
             
            // assign bid and tid to waited PACTs
            var currentBatchID = token.lastEmitTid + 1;

            for (int i = 0; i < this.deterministicRequests.Count; i++)
            {
                var tid = ++token.lastEmitTid;
                GenerateSchedulePerService(tid, currentBatchID, this.deterministicRequests[i]);
                this.detRequestPromise[i].SetResult(new Tuple<long, long>(currentBatchID, tid));
            }
            UpdateToken(token, currentBatchID, -1);

            deterministicRequests.Clear();
            detRequestPromise.Clear();
            return currentBatchID;
        }

        // GenerateSchedulePerService could be called as a:
        //
        // - Regional coordinator (Then 'service' refers to local coordinator)
        // - Local coordinator (Then 'service' refers to execution grains inside silo)
        //
        // This is why we differentiate between the two with isRegionalCoordinator
        public void GenerateSchedulePerService(long tid, long curBatchID, List<Tuple<int, string>> serviceList)
        {
            if (!this.bidToSubBatches.ContainsKey(curBatchID))
            {
                this.bidToSubBatches.Add(curBatchID, new Dictionary<Tuple<int, string>, SubBatch>());
                if (this.isRegionalCoordinator)
                {
                    // Maps: currentbatchID => <region, <local coordinator Tuple(id, region)>>
                    this.localCoordinatorPerSiloPerBatch.Add(curBatchID, new Dictionary<Tuple<int, string>, Tuple<int, string>>());
                } 
            }

            var serviceIDToSubBatch = this.bidToSubBatches[curBatchID];

            for (int i = 0; i < serviceList.Count; i++)
            {
                var serviceID = serviceList[i];
                if (serviceIDToSubBatch.ContainsKey(serviceID) == false)
                {
                    serviceIDToSubBatch.Add(serviceID, new SubBatch(curBatchID, myID));
                    if (this.isRegionalCoordinator)
                    {
                        // randomly choose a local coord as the coordinator for this batch on that silo
                        // Old code:
                        //var chosenCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToRandomCoordID(serviceID);
                        // New code:
                        int randomlyChosenLocalCoordinatorID = this.random.Next(Constants.numLocalCoordPerSilo);
                        string localCoordinatorServer = serviceID.Item2;
                        this.localCoordinatorPerSiloPerBatch[curBatchID].Add(serviceID, new Tuple<int, string>(randomlyChosenLocalCoordinatorID, localCoordinatorServer));
                    }
                }

                serviceIDToSubBatch[serviceID].txnList.Add(tid);
            }
        }

        public void UpdateToken(BasicToken token, long curBatchID, long globalBid)
        {
            var serviceIDToSubBatch = bidToSubBatches[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, serviceIDToSubBatch.Count);

            // update the last batch ID for each service accessed by this batch
            foreach (var serviceInfo in serviceIDToSubBatch)
            {
                var serviceID = serviceInfo.Key;
                var subBatch = serviceInfo.Value;

                if (token.lastBidPerService.ContainsKey(serviceID))
                {
                    subBatch.lastBid = token.lastBidPerService[serviceID];
                    // TODO: Consider this: token.lastGlobalBidPerGrain[new Tuple<int, string>(this.myID, serviceID)];
                    // the old code just used token.lastGlobalBidPerGrain[serviceID];
                    if (!this.isRegionalCoordinator) 
                    {
                        subBatch.lastGlobalBid = token.lastGlobalBidPerGrain[serviceID];
                    } 
                }
                // else, the default value is -1

                Debug.Assert(subBatch.bid > subBatch.lastBid);
                token.lastBidPerService[serviceID] = subBatch.bid;
                if (!this.isRegionalCoordinator) 
                {
                    token.lastGlobalBidPerGrain[serviceID] = globalBid;
                } 
            }
            bidToLastBid.Add(curBatchID, token.lastEmitBid);
            if (token.lastEmitBid != -1)
            {
                bidToLastCoordID.Add(curBatchID, token.lastCoordID);
            } 
            token.lastEmitBid = curBatchID;
            token.isLastEmitBidGlobal = globalBid != -1;
            token.lastCoordID = myID;
        }

        public void GarbageCollectTokenInfo(BasicToken token)
        {
            Debug.Assert(!this.isRegionalCoordinator);
            var expiredGrains = new HashSet<Tuple<int, string>>();

            // only when last batch is already committed, the next emitted batch can have its lastBid = -1 again
            foreach (var item in token.lastBidPerService) 
            {
                if (item.Value <= highestCommittedBid)
                { 
                     expiredGrains.Add(item.Key);
                }
            }
                
            foreach (var item in expiredGrains)
            {
                token.lastBidPerService.Remove(item);
                token.lastGlobalBidPerGrain.Remove(item);
            } 

            token.highestCommittedBid = highestCommittedBid;
        }

        public async Task WaitPrevBatchToCommit(long bid)
        {
            var lastBid = bidToLastBid[bid];
            bidToLastBid.Remove(bid);

            if (highestCommittedBid < lastBid)
            {
                var coord = bidToLastCoordID[bid];
                if (coord == myID)
                {
                    await WaitBatchCommit(lastBid);
                } 
                else
                {
                    if (this.isRegionalCoordinator)
                    {
                        var lastCoord = coordMap.GetGlobalCoord(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                    else // if it is a local coordinator
                    {
                        var lastCoord = coordMap.GetLocalCoord(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                }
            }
            else 
            {
                Debug.Assert(highestCommittedBid == lastBid);
            }

            if (bidToLastCoordID.ContainsKey(bid)) bidToLastCoordID.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            if (highestCommittedBid == bid) return;
            if (batchCommit.ContainsKey(bid) == false) batchCommit.Add(bid, new TaskCompletionSource<bool>());
            await batchCommit[bid].Task;
        }

        public void AckBatchCommit(long bid)
        {
            highestCommittedBid = Math.Max(bid, highestCommittedBid);
            if (batchCommit.ContainsKey(bid))
            {
                batchCommit[bid].SetResult(true);
                batchCommit.Remove(bid);
            }
        }
    }
}