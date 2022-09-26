
using System.Collections.Generic;
using Concurrency.Interface.Models;
using Microsoft.AspNetCore.Components.Forms;

namespace SnapperGeoReplication.Tests.SnapperGeoUnit.Tests
{
    public static class TestData
    {
        public static SubBatch CreateSubBatch(long bid, long coordinatorId, long previousBid, IList<long> transactions)
        {
            SubBatch subBatch = new SubBatch(bid, coordinatorId)
            {
                PreviousBid = previousBid
            };

            subBatch.Transactions.AddRange(transactions);

            return subBatch;
        }
    }
}