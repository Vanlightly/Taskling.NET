using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.ExecutionContext;
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
    public class When_FindDeadRangeBlocks
    {
        private int _taskDefinitionId;
        private string _taskExecution1;
        private string _executionTokenId;
        private string _block1;
        private string _block2;
        private string _block3;
        private string _block4;
        private string _block5;

        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, TestConstants.TaskName);
            
            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionTokenId = _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
        }

        private void InsertDateRangeTestData(TaskDeathMode taskDeathMode)
        {
            if(taskDeathMode == TaskDeathMode.Override)
                _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);
            else
                _taskExecution1 = _executionHelper.InsertKeepAliveTaskExecution(_taskDefinitionId);

            InsertDateRangeBlocksTestData();
        }

        private void InsertDateRangeBlocksTestData()
        {
            _block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-180), DateTime.UtcNow.AddMinutes(-181)).ToString();
            _block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-201)).ToString();
            _block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-220), DateTime.UtcNow.AddMinutes(-221)).ToString();
            _block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-240), DateTime.UtcNow.AddMinutes(-241)).ToString();
            _block5 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, DateTime.UtcNow.AddMinutes(-250), DateTime.UtcNow.AddMinutes(-251)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), DateTime.UtcNow.AddMinutes(-180), DateTime.UtcNow.AddMinutes(-185), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-205), BlockExecutionStatus.Started);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), DateTime.UtcNow.AddMinutes(-220), DateTime.UtcNow.AddMinutes(-225), BlockExecutionStatus.NotStarted);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), DateTime.UtcNow.AddMinutes(-240), DateTime.UtcNow.AddMinutes(-245), BlockExecutionStatus.Completed);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), DateTime.UtcNow.AddMinutes(-250), DateTime.UtcNow.AddMinutes(-255), BlockExecutionStatus.Started);
        }

        private void InsertNumericRangeTestData(TaskDeathMode taskDeathMode)
        {
            if(taskDeathMode == TaskDeathMode.Override)
                _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);
            else
                _taskExecution1 = _executionHelper.InsertKeepAliveTaskExecution(_taskDefinitionId);
            
            InsertNumericRangeBlocksTestData();
        }

        private void InsertNumericRangeBlocksTestData()
        {
            _block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, DateTime.UtcNow.AddMinutes(-100)).ToString();
            _block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 101, 200, DateTime.UtcNow.AddMinutes(-90)).ToString();
            _block3 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 201, 300, DateTime.UtcNow.AddMinutes(-80)).ToString();
            _block4 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 301, 400, DateTime.UtcNow.AddMinutes(-70)).ToString();
            _block5 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 401, 500, DateTime.UtcNow.AddMinutes(-60)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), DateTime.UtcNow.AddMinutes(-180), DateTime.UtcNow.AddMinutes(-185), BlockExecutionStatus.Failed);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-205), BlockExecutionStatus.Started);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), DateTime.UtcNow.AddMinutes(-220), DateTime.UtcNow.AddMinutes(-225), BlockExecutionStatus.NotStarted);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), DateTime.UtcNow.AddMinutes(-240), DateTime.UtcNow.AddMinutes(-245), BlockExecutionStatus.Completed);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), DateTime.UtcNow.AddMinutes(-250), DateTime.UtcNow.AddMinutes(-255), BlockExecutionStatus.Started);
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

        #region .: Date Range Blocks :.

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-300),
                DateTime.UtcNow.AddMinutes(-210),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(2, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block3));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block5));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_OverrideModeAndDateRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-500),
                DateTime.UtcNow.AddMinutes(-600),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(0, deadBlocks.Count);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 1;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-300),
                DateTime.UtcNow.AddMinutes(-10),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(1, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block5));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskDefinitionId, _taskExecution1, _executionTokenId, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-100),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(3, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block2));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block3));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block5));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskDefinitionId, _taskExecution1, _executionTokenId, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-100),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(2, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block3));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block5));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_KeepAliveModeAndDateRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskDefinitionId, _taskExecution1, _executionTokenId, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.DateRange,
                DateTime.UtcNow.AddMinutes(-100),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(0, deadBlocks.Count);
        }

        #endregion .: Date Range Blocks :.

        #region .: Numeric Range Blocks :.

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-300),
                DateTime.UtcNow.AddMinutes(-210),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(2, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block3));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block5));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_OverrideModeAndNumericRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-500),
                DateTime.UtcNow.AddMinutes(-600),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(0, deadBlocks.Count);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 1;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "",
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-300),
                DateTime.UtcNow.AddMinutes(-10),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(1, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block2));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskDefinitionId, _taskExecution1, _executionTokenId, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-100),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(3, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block2));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block3));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block5));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskDefinitionId, _taskExecution1, _executionTokenId, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-100),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(2, deadBlocks.Count);
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block2));
            Assert.IsTrue(deadBlocks.Any(x => x.RangeBlockId == _block3));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void When_KeepAliveModeAndNumericRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskDefinitionId, _taskExecution1, _executionTokenId, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                "1",
                BlockType.NumericRange,
                DateTime.UtcNow.AddMinutes(-100),
                blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = sut.FindDeadRangeBlocks(request);

            // ASSERT
            Assert.AreEqual(0, deadBlocks.Count);
        }

        #endregion .: Date Range Blocks :.
    }
}
