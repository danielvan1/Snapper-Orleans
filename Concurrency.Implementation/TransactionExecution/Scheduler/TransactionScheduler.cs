using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{
    public class TransactionScheduler
    {
        private readonly ScheduleInfoManager scheduleInfoManager;
        private readonly Dictionary<long, SubBatch> batchInfo;                               // key: local bid
        private readonly Dictionary<long, long> tidToLastTid;
        private readonly Dictionary<long, TaskCompletionSource<bool>> deterministicExecutionPromise;   // key: local tid

        public TransactionScheduler()
        {
            this.scheduleInfoManager = new ScheduleInfoManager();
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
                // TODO: Change it to look nicer
                if (i == 0) tidToLastTid.Add(tid, -1);
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
                // if the tid is the first txn in the batch, wait for previous node
                var previousNode = this.scheduleInfoManager.GetDependingNode(bid);
                await previousNode.NextNodeCanExecute.Task;
            }
            else
            {
                // wait for previous det txn
                await this.deterministicExecutionPromise[previousTid].Task;
                this.deterministicExecutionPromise.Remove(previousTid);
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
            var transactions = this.batchInfo[bid].Transactions;
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