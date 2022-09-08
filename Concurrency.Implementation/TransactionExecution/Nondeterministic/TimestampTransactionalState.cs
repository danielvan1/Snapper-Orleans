﻿using System;
using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.TransactionExecution.Nondeterministic;

namespace Concurrency.Implementation.TransactionExecution.Nondeterministic
{
    // all RW transactions commit in timestamp order
    public class TimestampTransactionalState<TState> : INonDetTransactionalState<TState> where TState : ICloneable, new()
    {
        private long lastCommitTid;
        private DLinkedList<TransactionStateInfo> transactionList;
        private Dictionary<long, Node<TransactionStateInfo>> transactionMap;
        private Dictionary<long, long> readDependencyMap;

        public TimestampTransactionalState()
        {
            lastCommitTid = -1;
            transactionList = new DLinkedList<TransactionStateInfo>();
            transactionMap = new Dictionary<long, Node<TransactionStateInfo>>();
            readDependencyMap = new Dictionary<long, long>();
        }

        public void CheckGC()
        {
            if (transactionList.size != 0) Console.WriteLine($"TimestampTransactionalState: transactionList.Count = {transactionList.size}");
            if (transactionMap.Count != 0) Console.WriteLine($"TimestampTransactionalState: transactionMap.Count = {transactionMap.Count}");
            if (readDependencyMap.Count != 0) Console.WriteLine($"TimestampTransactionalState: readDependencyMap.Count = {readDependencyMap.Count}");
        }

        public Task<TState> ReadWrite(long tid, TState committedState)
        {
            TState state;
            TState copy;
            TransactionStateInfo info;
            Node<TransactionStateInfo> node;
            if (transactionMap.ContainsKey(tid))  // if tid has written the state before
            {
                Debug.Assert(transactionMap[tid].data.status.Equals(Status.Executing));
                if (transactionMap[tid].data.rts > tid) throw new DeadlockAvoidanceException($"Transaction {tid} fail to write because a more recent transaction has read. ");
                return Task.FromResult(transactionMap[tid].data.state);
            }
            var lastNode = findLastNonAbortedTransaction();
            if (lastNode == null)    // either the transactionMap is empty or all nodes have been aborted
            {
                state = committedState;
                copy = (TState)state.Clone();
                info = new TransactionStateInfo(tid, -1, tid, Status.Executing, copy);
                node = transactionList.Append(info);
                transactionMap.Add(tid, node);
                return Task.FromResult(copy);
            }
            if (lastNode.data.status.Equals(Status.Committed))
            {
                if (lastNode.data.tid != transactionList.head.data.tid) Debug.Assert(transactionList.head.data.status.Equals(Status.Aborted));
            }
            else Debug.Assert(lastNode.data.status.Equals(Status.Executing));

            if (tid < lastNode.data.tid) throw new DeadlockAvoidanceException($"Transaction {tid} fail to write because a more recent transaction has written. ");
            if (tid < lastNode.data.rts) throw new DeadlockAvoidanceException($"Transaction {tid} fail to write because a more recent transaction has read. ");
            if (readDependencyMap.ContainsKey(tid))    // if tid has read a version before
            {
                var prev = readDependencyMap[tid];
                if (prev != lastNode.data.tid) throw new DeadlockAvoidanceException($"Transaction {tid} is aborted to enforce repeatable read. ");
                Debug.Assert(lastNode.data.rts >= prev);
            }
            // If tid hasn't accessed this grain before
            lastNode.data.rts = tid;
            state = lastNode.data.state;
            copy = (TState)state.Clone();
            info = new TransactionStateInfo(tid, lastNode.data.tid, tid, Status.Executing, copy);
            node = transactionList.Append(info);
            transactionMap.Add(tid, node);
            return Task.FromResult(copy);
        }

        public Task<TState> Read(long tid, TState committedState)
        {
            TState state;
            TState copy;
            TransactionStateInfo info;
            Node<TransactionStateInfo> node;
            if (transactionMap.ContainsKey(tid))  // if tid has written the state before
            {
                Debug.Assert(transactionMap[tid].data.status.Equals(Status.Executing));
                return Task.FromResult(transactionMap[tid].data.state);
            }
            var lastNode = findLastNonAbortedTransaction();
            if (lastNode == null)   // either the transactionMap is empty or all nodes have been aborted
            {
                state = committedState;
                copy = (TState)state.Clone();
                info = new TransactionStateInfo(-1, -1, tid, Status.Executing, copy);
                node = transactionList.Append(info);
                node.data.ExecutionPromise.SetResult(true);
                node.data.status = Status.Committed;
                readDependencyMap.Add(tid, -1);
                return Task.FromResult(copy);
            }
            if (lastNode.data.status.Equals(Status.Committed))
            {
                if (lastNode.data.tid != transactionList.head.data.tid) Debug.Assert(transactionList.head.data.status.Equals(Status.Aborted));
            }
            else Debug.Assert(lastNode.data.status.Equals(Status.Executing));

            if (tid < lastNode.data.tid) throw new DeadlockAvoidanceException($"Transaction {tid} fail to read because a more recent transaction has written. ");
            if (readDependencyMap.ContainsKey(tid))   // if tid has read the state before
            {
                var prev = readDependencyMap[tid];
                if (prev != lastNode.data.tid) throw new DeadlockAvoidanceException($"Transaction {tid} fail to read to enforce repeatable read. ");
            }
            else readDependencyMap.Add(tid, lastNode.data.tid);
            lastNode.data.rts = Math.Max(lastNode.data.rts, tid);
            return Task.FromResult(lastNode.data.state);
        }

        private Node<TransactionStateInfo> findLastNonAbortedTransaction()
        {
            Node<TransactionStateInfo> lastNode = transactionList.tail;
            while (lastNode != null)
            {
                if (lastNode.data.status.Equals(Status.Aborted)) lastNode = lastNode.prev;
                else break;
            }
            return lastNode;
        }

        public async Task<bool> Prepare(long tid, bool isReader)   // check if the dependent transaction has committed
        {
            if (isReader) Debug.Assert(readDependencyMap.ContainsKey(tid));
            else Debug.Assert(transactionMap.ContainsKey(tid));

            long depTid;
            if (isReader) depTid = readDependencyMap[tid];   // tid is a read-only transaction
            else depTid = transactionMap[tid].data.depTid;   // tid is a read-write transaction

            if (depTid <= lastCommitTid) return true;

            Debug.Assert(transactionMap.ContainsKey(depTid));
            var info = transactionMap[depTid].data;
            await info.ExecutionPromise.Task;
            if (info.status.Equals(Status.Committed)) return true;
            if (info.status.Equals(Status.Aborted)) return false;
            Debug.Assert(false);    // this should not happen
            return false;
        }

        public void Commit(long tid, TState committedState)
        {
            Debug.Assert(readDependencyMap.ContainsKey(tid) || transactionMap.ContainsKey(tid));
            lastCommitTid = Math.Max(lastCommitTid, tid);
            if (readDependencyMap.ContainsKey(tid))
            {
                Debug.Assert(readDependencyMap[tid] <= lastCommitTid);
                readDependencyMap.Remove(tid);
            }
            if (!transactionMap.ContainsKey(tid)) return;

            // Commit read-write transactions (tid's all dependent transactions must have already committed)
            var node = transactionMap[tid];
            node.data.status = Status.Committed;
            // Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);

            // Clean the transaction list
            CleanUp(node);
            committedState = node.data.state;
        }

        public void Abort(long tid)
        {
            // It's possible that tid is not in readDependencyMap and transactionMap
            if (readDependencyMap.ContainsKey(tid)) readDependencyMap.Remove(tid);

            // (1) tis is a read-only transaction
            // (2) tid didn't succeed to write any version
            if (!transactionMap.ContainsKey(tid)) return;

            // Abort read-write transactions
            var node = transactionMap[tid];
            node.data.status = Status.Aborted;
            //Set the promise of transaction tid, such that transactions depending on it can prepare.
            node.data.ExecutionPromise.SetResult(true);
        }

        // Clean transactions that are before the committed transaction
        private void CleanUp(Node<TransactionStateInfo> node)   // node represents a newly committed transaction
        {
            var curNode = transactionList.head;
            while (curNode != null)
            {
                if (curNode.data.tid == node.data.tid) return;
                if (curNode.data.tid < node.data.tid && (curNode.data.status.Equals(Status.Aborted) || curNode.data.status.Equals(Status.Committed)))
                {
                    transactionList.Remove(curNode);
                    transactionMap.Remove(curNode.data.tid);
                }
                curNode = curNode.next;
            }
        }

        public TState GetPreparedState(long tid)
        {
            // tid must be a read-write transaction
            return transactionMap[tid].data.state;
        }

        public enum Status
        {
            Executing,
            Aborted,
            Committed
        }

        private class TransactionStateInfo
        {

            public long tid { get; set; }
            public Status status { get; set; }

            public TState state { get; set; }
            public long rts { get; set; }

            public long depTid { get; set; }

            public TaskCompletionSource<bool> ExecutionPromise { get; set; }

            public TransactionStateInfo(long tid, long depTid, long rts, Status status, TState copy)
            {
                this.tid = tid;
                this.depTid = depTid;
                this.status = status;
                this.state = copy;
                this.rts = rts;
                ExecutionPromise = new TaskCompletionSource<bool>();
            }

            public TransactionStateInfo(long tid, Status status)
            {
                this.tid = tid;
                this.status = status;
                ExecutionPromise = new TaskCompletionSource<bool>();
            }
        }
    }
}