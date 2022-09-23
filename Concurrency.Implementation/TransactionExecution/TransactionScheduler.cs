using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Concurrency.Interface.Models;

namespace Concurrency.Implementation.TransactionExecution
{
    public class TransactionScheduler
    {
        readonly int myID;
        public ScheduleInfo scheduleInfo;
        Dictionary<long, SubBatch> batchInfo;                               // key: local bid
        Dictionary<long, long> tidToLastTid;
        Dictionary<long, TaskCompletionSource<bool>> detExecutionPromise;   // key: local tid

        public TransactionScheduler(int myID)
        {
            this.myID = myID;
            scheduleInfo = new ScheduleInfo(myID);
            batchInfo = new Dictionary<long, SubBatch>();
            tidToLastTid = new Dictionary<long, long>();
            detExecutionPromise = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        public void CheckGC()
        {
            scheduleInfo.CheckGC();
            if (batchInfo.Count != 0) Console.WriteLine($"TransactionScheduler: batchInfo.Count = {batchInfo.Count}");
            if (tidToLastTid.Count != 0) Console.WriteLine($"TransactionScheduler: tidToLastTid.Count = {tidToLastTid.Count}");
            if (detExecutionPromise.Count != 0) Console.WriteLine($"TransactionScheduler: detExecutionPromise.Count = {detExecutionPromise.Count}");
        }

        public void RegisterBatch(SubBatch batch, long globalBid, long highestCommittedBid)
        {
            this.scheduleInfo.InsertDeterministicBatch(batch, globalBid, highestCommittedBid);
            this.batchInfo.Add(batch.bid, batch);

            // TODO: This logic can be improved
            for (int i = 0; i < batch.transactions.Count; i++)
            {
                var tid = batch.transactions[i];
                // TODO: Change it to look nicer
                if (i == 0) tidToLastTid.Add(tid, -1);
                else tidToLastTid.Add(tid, batch.transactions[i - 1]);

                if (i == batch.transactions.Count - 1) break;

                // the last txn in the list does not need this entry
                if (!this.detExecutionPromise.ContainsKey(tid))
                    this.detExecutionPromise.Add(tid, new TaskCompletionSource<bool>());
            }
        }

        public async Task WaitForTurn(long bid, long tid)
        {
            var depTid = tidToLastTid[tid];
            if (depTid == -1)
            {
                // if the tid is the first txn in the batch, wait for previous node
                var depNode = scheduleInfo.GetDependingNode(bid);
                await depNode.nextNodeCanExecute.Task;
            }
            else
            {
                // wait for previous det txn
                await detExecutionPromise[depTid].Task;
                detExecutionPromise.Remove(depTid);
            }
        }

        public async Task WaitForTurn(long tid)
        {
            await scheduleInfo.InsertNonDetTransaction(tid).nextNodeCanExecute.Task;
        }

        // int: the local coordID if the batch has been completed
        public long AckComplete(long bid, long tid)
        {
            var txnList = batchInfo[bid].transactions;
            Debug.Assert(txnList.First() == tid);
            txnList.RemoveAt(0);
            tidToLastTid.Remove(tid);

            long coordID = -1;
            if (txnList.Count == 0)
            {
                coordID = batchInfo[bid].coordID;
                batchInfo.Remove(bid);
            }

            else detExecutionPromise[tid].SetResult(true);

            return coordID;
        }

        // this function is only used to do grabage collection
        // bid: current highest committed batch among all coordinators
        public void AckBatchCommit(long bid)
        {
            if (bid == -1) return;
            var head = scheduleInfo.detNodes[-1];
            var node = head.next;
            if (node == null) return;
            while (node != null)
            {
                if (node.isDet)
                {
                    if (node.id <= bid)
                    {
                        scheduleInfo.detNodes.Remove(node.id);
                        scheduleInfo.localBidToGlobalBid.Remove(node.id);
                    }
                    else break;   // meet a det node whose id > bid
                }
                else
                {
                    if (node.next != null)
                    {
                        Debug.Assert(node.next.isDet);  // next node must be a det node
                        if (node.next.id <= bid)
                        {
                            scheduleInfo.nonDetNodes.Remove(node.id);
                            scheduleInfo.nonDetNodeIDToTxnSet.Remove(node.id);
                        }
                        else break;
                    }
                }
                if (node.next == null) break;
                node = node.next;
            }

            // case 1: node.isDet = true && node.id <= bid && node.next = null
            if (node.isDet && node.id <= bid)    // node should be removed
            {
                Debug.Assert(node.next == null);
                head.next = null;
            }
            else  // node.isDet = false || node.id > bid
            {
                // case 2: node.isDet = true && node.id > bid
                // case 3: node.isDet = false && node.next = null
                // case 4: node.isDet = false && node.next.id > bid
                Debug.Assert((node.isDet && node.id > bid) || (!node.isDet && (node.next == null || node.next.id > bid)));
                head.next = node;
                node.prev = head;
            }
        }
    }
}