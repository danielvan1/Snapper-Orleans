using System;
using System.Collections.Generic;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Implementation.TransactionExecution.Scheduler;
using Concurrency.Interface.Models;
using Xunit;

namespace SnapperGeoReplication.Tests.SnapperGeoUnit.Tests
{
    public class SchuduleInfoTests
    {

        [Fact]
        public void HappyPath()
        {
            // Arrange
            long coordinatorId = 4;
            long firstBid = 0; long firstPreviousBid = -1;
            long secondBid = 3; long secondPreviousBid = 0;

            SubBatch subBatchFirst = TestData.CreateSubBatch(firstBid, coordinatorId,firstPreviousBid, new List<long>() { 0, 1, 2 });
            SubBatch subBatchSecond = TestData.CreateSubBatch(secondBid, coordinatorId, secondPreviousBid, new List<long>() { 3, 4 });

            ScheduleInfoManager scheduleInfo = new ScheduleInfoManager();
            scheduleInfo.InsertDeterministicBatch(subBatchFirst, 0, -1);
            scheduleInfo.InsertDeterministicBatch(subBatchSecond, 3, 3);

            // Act
            ScheduleNode firstPreviousNode = scheduleInfo.GetDependingNode(0);
            ScheduleNode secondPreviousNode = scheduleInfo.GetDependingNode(3);

            // Assert
            Assert.Equal(secondPreviousNode.Bid, secondPreviousBid);
            Assert.Equal(firstPreviousNode.Bid, firstPreviousBid);
        }
    }
}