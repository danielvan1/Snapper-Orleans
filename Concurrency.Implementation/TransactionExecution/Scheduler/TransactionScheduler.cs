using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public class TransactionScheduler : ITransactionScheduler
    {
        // private readonly ScheduleInfoManager scheduleInfoManager;
        private readonly Dictionary<long, SubBatch> batchInfo;                               // key: local bid
        private readonly Dictionary<long, long> tidToLastTid;
        private readonly Dictionary<long, TaskCompletionSource<bool>> deterministicExecutionPromise;   // key: local tid
        private readonly ILogger<TransactionScheduler> logger;
        private readonly IScheduleInfoManager scheduleInfoManager;
        private readonly GrainReference grainReference;

        public TransactionScheduler(ILogger<TransactionScheduler> logger, IScheduleInfoManager scheduleInfoManager, GrainReference grainReference)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.scheduleInfoManager = scheduleInfoManager ?? throw new ArgumentNullException(nameof(scheduleInfoManager));
            this.grainReference = grainReference ?? throw new ArgumentNullException(nameof(grainReference));
            this.batchInfo = new Dictionary<long, SubBatch>();
            this.tidToLastTid = new Dictionary<long, long>();
            this.deterministicExecutionPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        public void CompleteDeterministicBatch(long bid)
        {
            this.scheduleInfoManager.CompleteDeterministicBatch(bid);
        }

        public void RegisterBatch(SubBatch batch, long regionalBid, long highestCommittedBid)
        {
            this.scheduleInfoManager.InsertDeterministicBatch(batch, regionalBid, highestCommittedBid);
            this.batchInfo.Add(batch.Bid, batch);

            // TODO: This logic can be improved
            for (int i = 0; i < batch.Transactions.Count; i++)
            {
                var tid = batch.Transactions[i];
                // TODO: rafactor
                if (i == 0) this.tidToLastTid.Add(tid, -1);
                else tidToLastTid.Add(tid, batch.Transactions[i - 1]);

                if (i == batch.Transactions.Count - 1) break;

                // the last txn in the list does not need this entry
                if (!this.deterministicExecutionPromise.ContainsKey(tid))
                {
                    this.deterministicExecutionPromise.Add(tid, new TaskCompletionSource<bool>());
                }
            }
        }

        public async Task WaitForTurn(long bid, long tid)
        {
            var previousTid = this.tidToLastTid[tid];

            if (previousTid == -1)
            {
                // this.logger.LogInformation("First transaction in the batch {bid} with tid: {tid}", bid, tid);
                this.logger.LogError("First transaction in the batch {bid} with tid: {tid}", this.grainReference, bid, tid);
                // if the tid is the first txn in the batch, wait for previous node
                var previousNode = this.scheduleInfoManager.GetDependingNode(bid);
                await previousNode.NextNodeCanExecute.Task;
            }
            else
            {
                // this.logger.LogInformation("Waiting for previousTid: {prev} to finish. Current tid is: {tid} in batch: {bid}", previousTid, tid, bid);
                this.logger.LogError("Waiting for previousTid: {prev} to finish. Current tid is: {tid} in batch: {bid}", this.grainReference,  previousTid, tid, bid);
                // wait for previous det txn
                await this.deterministicExecutionPromise[previousTid].Task;
                this.deterministicExecutionPromise.Remove(previousTid);

                this.logger.LogError("Done waiting for previousTid: {prev} to finish. Current tid is: {tid} in batch: {bid}", this.grainReference,  previousTid, tid, bid);
                // this.logger.LogInformation("Done waiting for previousTid: {prev} to finish. Current tid is: {tid} in batch: {bid}", previousTid, tid, bid);
            }
        }

        /// <summary>
        /// Determines whether a batch is complete.
        /// If it is complete it returns the coordinator belonging to the subbatch.
        /// otherwise -1.
        /// </summary>
        public long IsBatchComplete(long bid, long tid)
        {
            // TODO: Can be optimized to use a Queue. This is O(n^2).
            List<long> transactions = this.batchInfo[bid].Transactions;

            Debug.Assert(transactions.First() == tid);
            transactions.RemoveAt(0);
            this.tidToLastTid.Remove(tid);

            long coordinatorId = -1;

            if (transactions.Count == 0)
            {
                coordinatorId = this.batchInfo[bid].CoordinatorId;
                this.batchInfo.Remove(bid);
            }
            else
            {
                // The next Tid can continue after this.
                this.deterministicExecutionPromise[tid].SetResult(true);
            }

            return coordinatorId;
        }

        // this function is only used to do grabage collection
        // bid: current highest committed batch among all coordinators
        public void GarbageCollection(long bid)
        {
            this.scheduleInfoManager.GarbageCollection(bid);
        }
    }
}