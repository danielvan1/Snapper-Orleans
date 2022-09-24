using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionExecution
{
    public class TransactionScheduler
    {
        private readonly ScheduleInfo scheduleInfo;
        private Dictionary<long, SubBatch> batchInfo;                               // key: local bid
        private Dictionary<long, long> tidToLastTid;
        private Dictionary<long, TaskCompletionSource<bool>> deterministicExecutionPromise;   // key: local tid

        public TransactionScheduler()
        {
            this.scheduleInfo = new ScheduleInfo();
            this.batchInfo = new Dictionary<long, SubBatch>();
            this.tidToLastTid = new Dictionary<long, long>();
            this.deterministicExecutionPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        public void CompleteDeterministicBatch(long bid)
        {
            this.scheduleInfo.CompleteDeterministicBatch(bid);
        }

        public void RegisterBatch(SubBatch batch, long regionalBid, long highestCommittedBid)
        {
            this.scheduleInfo.InsertDeterministicBatch(batch, regionalBid, highestCommittedBid);
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
                var previousNode = this.scheduleInfo.GetDependingNode(bid);
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

        // TODO: THIS IS GARBAGE COLLECTION. METHOD NAME SHOULD BE RENAMED TO SOME PROPER
        // this function is only used to do grabage collection
        // bid: current highest committed batch among all coordinators
        public void AckBatchCommit(long bid)
        {
            if (bid == -1) return;
            var head = this.scheduleInfo.DeterministicNodes[-1];
            var node = head.Next;
            if (node == null) return;
            while (node != null)
            {
                if (node.Id <= bid)
                {
                    scheduleInfo.DeterministicNodes.Remove(node.Id);
                    scheduleInfo.LocalBidToRegionalBid.Remove(node.Id);
                } else break;   // meet a det node whose id > bid

                if (node.Next == null) break;
                node = node.Next;
            }

            // case 1: node.isDet = true && node.id <= bid && node.next = null
            if (node.IsDet && node.Id <= bid)    // node should be removed
            {
                Debug.Assert(node.Next == null);
                head.Next = null;
            }
            else  // node.isDet = false || node.id > bid
            {
                // case 2: node.isDet = true && node.id > bid
                // case 3: node.isDet = false && node.next = null
                // case 4: node.isDet = false && node.next.id > bid
                Debug.Assert((node.IsDet && node.Id > bid) || (!node.IsDet && (node.Next == null || node.Next.Id > bid)));
                head.Next = node;
                node.Previous = head;
            }
        }
    }
}