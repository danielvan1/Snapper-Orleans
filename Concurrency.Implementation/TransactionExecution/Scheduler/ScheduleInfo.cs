﻿using System.Collections.Generic;
using System.Diagnostics;
using Concurrency.Interface.Models;
using Microsoft.Extensions.Logging;

namespace Concurrency.Implementation.TransactionExecution.Scheduler
{

    public class ScheduleInfoManager : IScheduleInfoManager
    {
        private readonly Dictionary<long, ScheduleNode> deterministicNodes;     // node ID: batch ID (local bid generated by local coordinators)
        // private readonly Dictionary<long, long> localBidToRegionalBid;   // local bid, regional bid
        private readonly ILogger<ScheduleInfoManager> logger;

        public ScheduleInfoManager(ILogger<ScheduleInfoManager> logger)
        {
            this.deterministicNodes = new Dictionary<long, ScheduleNode>();
            // this.localBidToRegionalBid = new Dictionary<long, long>();

            var node = new ScheduleNode(-1, true);
            node.NextNodeCanExecute.SetResult(true);

            this.deterministicNodes.Add(-1, node);
            // this.localBidToRegionalBid.Add(-1, -1);
            this.logger = logger ?? throw new System.ArgumentNullException(nameof(logger));
        }


        public void InsertDeterministicBatch(SubBatch subBatch, long regionalBid, long highestCommittedBid)
        {
            ScheduleNode node;

            if (!this.deterministicNodes.ContainsKey(subBatch.Bid))
            {
                node = new ScheduleNode(subBatch.Bid, true);
                this.deterministicNodes.Add(subBatch.Bid, node);
                // this.localBidToRegionalBid.Add(subBatch.Bid, regionalBid);
            }
            else
            {
                node = this.deterministicNodes[subBatch.Bid];
            }

            if (this.deterministicNodes.ContainsKey(subBatch.PreviousBid))
            {
                var previousNode = this.deterministicNodes[subBatch.PreviousBid];
                if (previousNode.Next == null)
                {
                    previousNode.Next = node;
                    node.Previous = previousNode;
                }
                else
                {
                    Debug.Assert(!previousNode.Next.IsDet && previousNode.Next.Next == null);
                    previousNode.Next.Next = node;
                    node.Previous = previousNode.Next;
                }
            }
            else
            {
                // last node is already deleted because it's committed
                if (highestCommittedBid >= subBatch.PreviousBid)
                {
                    var previousNode = this.deterministicNodes[-1];
                    if (previousNode.Next == null)
                    {
                        previousNode.Next = node;
                        node.Previous = previousNode;
                    }
                    else
                    {
                        Debug.Assert(!previousNode.Next.IsDet && previousNode.Next.Next == null);
                        previousNode.Next.Next = node;
                        node.Previous = previousNode.Next;
                    }
                }
                else   // last node hasn't arrived yet
                {
                    var prevNode = new ScheduleNode(subBatch.PreviousBid, true);
                    this.deterministicNodes.Add(subBatch.PreviousBid, prevNode);
                    // this.localBidToRegionalBid.Add(subBatch.PreviousBid, subBatch.previousGlobalBid);
                    prevNode.Next = node;
                    node.Previous = prevNode;
                    Debug.Assert(prevNode.Previous == null);
                }
            }
        }

        public ScheduleNode GetDependingNode(long bid)
        {
            return this.deterministicNodes[bid].Previous;
        }

        public void CompleteDeterministicBatch(long bid)
        {
            this.deterministicNodes[bid].NextNodeCanExecute.SetResult(true);
        }

        public void GarbageCollection(long bid)
        {
            if (bid == -1) return;
            var head = this.deterministicNodes[-1];
            var node = head.Next;
            if (node == null) return;
            while (node != null)
            {
                if (node.Bid <= bid)
                {
                    this.deterministicNodes.Remove(node.Bid);
                    // this.localBidToRegionalBid.Remove(node.Bid);
                }
                else break;   // meet a det node whose id > bid

                if (node.Next == null) break;
                node = node.Next;
            }

            // case 1: node.isDet = true && node.id <= bid && node.next = null
            if (node.IsDet && node.Bid <= bid)    // node should be removed
            {
                Debug.Assert(node.Next == null);
                head.Next = null;
            }
            else  // node.isDet = false || node.id > bid
            {
                // case 2: node.isDet = true && node.id > bid
                // case 3: node.isDet = false && node.next = null
                // case 4: node.isDet = false && node.next.id > bid
                Debug.Assert((node.IsDet && node.Bid > bid) || (!node.IsDet && (node.Next == null || node.Next.Bid > bid)));
                head.Next = node;
                node.Previous = head;
            }
        }
    }
}