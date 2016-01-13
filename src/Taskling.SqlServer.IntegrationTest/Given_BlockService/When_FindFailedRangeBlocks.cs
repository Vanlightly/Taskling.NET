using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.InfrastructureContracts.Blocks.RangeBlocks;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Given_BlockService
{
    [TestClass]
    public class When_FindFailedRangeBlocks
    {
        private int _taskDefinitionId;
        private int _taskExecutionId;
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        [TestInitialize]
        public void Initialize()
        {
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName, TestConstants.TaskName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
        }

        private BlockService CreateSut()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 1, 1)
            };
            return new BlockService(settings, new TaskService(settings));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_FailedBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
        {
            // ARRANGE
            var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);
            var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-2), DateTime.UtcNow.AddMinutes(-1));
            var block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-12), DateTime.UtcNow.AddMinutes(-11));
            var blockExecution1 = _blocksHelper.InsertBlockExecution(taskExecution1, block1, DateTime.UtcNow.AddMinutes(-2), DateTime.UtcNow.AddMinutes(-1), BlockExecutionStatus.Failed);
            var blockExecution2 = _blocksHelper.InsertBlockExecution(taskExecution1, block2, DateTime.UtcNow.AddMinutes(-12), DateTime.UtcNow.AddMinutes(-11), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-20),
                2);
            
            // ACT
            var sut = CreateSut();
            var failedBlocks = sut.FindFailedRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(1, failedBlocks.Count);
            Assert.AreEqual(block1.ToString(), failedBlocks[0].RangeBlockId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_FailedBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
        {
            // ARRANGE
            var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);
            var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-2), DateTime.UtcNow.AddMinutes(-1));
            var block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-12), DateTime.UtcNow.AddMinutes(-11));
            var block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-22), DateTime.UtcNow.AddMinutes(-21));
            var block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-32), DateTime.UtcNow.AddMinutes(-31));
            _blocksHelper.InsertBlockExecution(taskExecution1, block1, DateTime.UtcNow.AddMinutes(-2), DateTime.UtcNow.AddMinutes(-1), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block2, DateTime.UtcNow.AddMinutes(-12), DateTime.UtcNow.AddMinutes(-11), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block3, DateTime.UtcNow.AddMinutes(-22), DateTime.UtcNow.AddMinutes(-21), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(taskExecution1, block4, DateTime.UtcNow.AddMinutes(-32), DateTime.UtcNow.AddMinutes(-31), BlockExecutionStatus.Completed);

            int blockCountLimit = 2;

            var request = new FindFailedBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-200),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var failedBlocks = sut.FindFailedRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(blockCountLimit, failedBlocks.Count);
            Assert.IsTrue(failedBlocks.Any(x => x.RangeBlockId == block2.ToString()));
            Assert.IsTrue(failedBlocks.Any(x => x.RangeBlockId == block3.ToString()));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_FailedBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            var taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);
            var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-201));
            var block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-212), DateTime.UtcNow.AddMinutes(-211));
            var blockExecution1 = _blocksHelper.InsertBlockExecution(taskExecution1, block1, DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-201), BlockExecutionStatus.Failed);
            var blockExecution2 = _blocksHelper.InsertBlockExecution(taskExecution1, block2, DateTime.UtcNow.AddMinutes(-212), DateTime.UtcNow.AddMinutes(-211), BlockExecutionStatus.Completed);

            var request = new FindFailedBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-20),
                2);

            // ACT
            var sut = CreateSut();
            var failedBlocks = sut.FindFailedRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(0, failedBlocks.Count);
        }
    }
}
