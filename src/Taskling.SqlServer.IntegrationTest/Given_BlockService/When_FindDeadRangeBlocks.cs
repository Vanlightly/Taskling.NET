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
        private int _taskSecondaryId;
        private int _taskExecution1;
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
            
            _taskSecondaryId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertUnlimitedExecutionToken(_taskSecondaryId);
        }

        private void InsertDateRangeTestData()
        {
            _taskExecution1 = _executionHelper.InsertTaskExecution(_taskSecondaryId);
            _block1 = _blocksHelper.InsertDateRangeBlock(_taskSecondaryId, DateTime.UtcNow.AddMinutes(-180), DateTime.UtcNow.AddMinutes(-181)).ToString();
            _block2 = _blocksHelper.InsertDateRangeBlock(_taskSecondaryId, DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-201)).ToString();
            _block3 = _blocksHelper.InsertDateRangeBlock(_taskSecondaryId, DateTime.UtcNow.AddMinutes(-220), DateTime.UtcNow.AddMinutes(-221)).ToString();
            _block4 = _blocksHelper.InsertDateRangeBlock(_taskSecondaryId, DateTime.UtcNow.AddMinutes(-240), DateTime.UtcNow.AddMinutes(-241)).ToString();
            _block5 = _blocksHelper.InsertDateRangeBlock(_taskSecondaryId, DateTime.UtcNow.AddMinutes(-250), DateTime.UtcNow.AddMinutes(-251)).ToString();
            _blocksHelper.InsertDateRangeBlockExecution(_taskExecution1, long.Parse(_block1), DateTime.UtcNow.AddMinutes(-180), DateTime.UtcNow.AddMinutes(-185), BlockExecutionStatus.Failed);
            _blocksHelper.InsertDateRangeBlockExecution(_taskExecution1, long.Parse(_block2), DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-205), BlockExecutionStatus.Started);
            _blocksHelper.InsertDateRangeBlockExecution(_taskExecution1, long.Parse(_block3), DateTime.UtcNow.AddMinutes(-220), DateTime.UtcNow.AddMinutes(-225), BlockExecutionStatus.NotStarted);
            _blocksHelper.InsertDateRangeBlockExecution(_taskExecution1, long.Parse(_block4), DateTime.UtcNow.AddMinutes(-240), DateTime.UtcNow.AddMinutes(-245), BlockExecutionStatus.Completed);
            _blocksHelper.InsertDateRangeBlockExecution(_taskExecution1, long.Parse(_block5), DateTime.UtcNow.AddMinutes(-250), DateTime.UtcNow.AddMinutes(-255), BlockExecutionStatus.Started);
        }

        private void InsertNumericRangeTestData()
        {
            _taskExecution1 = _executionHelper.InsertTaskExecution(_taskSecondaryId);
            _block1 = _blocksHelper.InsertNumericRangeBlock(_taskSecondaryId, 1, 100, DateTime.UtcNow.AddMinutes(-100)).ToString();
            _block2 = _blocksHelper.InsertNumericRangeBlock(_taskSecondaryId, 101, 200, DateTime.UtcNow.AddMinutes(-90)).ToString();
            _block3 = _blocksHelper.InsertNumericRangeBlock(_taskSecondaryId, 201, 300, DateTime.UtcNow.AddMinutes(-80)).ToString();
            _block4 = _blocksHelper.InsertNumericRangeBlock(_taskSecondaryId, 301, 400, DateTime.UtcNow.AddMinutes(-70)).ToString();
            _block5 = _blocksHelper.InsertNumericRangeBlock(_taskSecondaryId, 401, 500, DateTime.UtcNow.AddMinutes(-60)).ToString();
            _blocksHelper.InsertNumericRangeBlockExecution(_taskExecution1, long.Parse(_block1), DateTime.UtcNow.AddMinutes(-180), DateTime.UtcNow.AddMinutes(-185), BlockExecutionStatus.Failed);
            _blocksHelper.InsertNumericRangeBlockExecution(_taskExecution1, long.Parse(_block2), DateTime.UtcNow.AddMinutes(-200), DateTime.UtcNow.AddMinutes(-205), BlockExecutionStatus.Started);
            _blocksHelper.InsertNumericRangeBlockExecution(_taskExecution1, long.Parse(_block3), DateTime.UtcNow.AddMinutes(-220), DateTime.UtcNow.AddMinutes(-225), BlockExecutionStatus.NotStarted);
            _blocksHelper.InsertNumericRangeBlockExecution(_taskExecution1, long.Parse(_block4), DateTime.UtcNow.AddMinutes(-240), DateTime.UtcNow.AddMinutes(-245), BlockExecutionStatus.Completed);
            _blocksHelper.InsertNumericRangeBlockExecution(_taskExecution1, long.Parse(_block5), DateTime.UtcNow.AddMinutes(-250), DateTime.UtcNow.AddMinutes(-255), BlockExecutionStatus.Started);
        }

        private BlockService CreateSut()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                TableSchema = "Taskling",
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
            InsertDateRangeTestData();
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertDateRangeTestData();
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertDateRangeTestData();
            int blockCountLimit = 1;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertDateRangeTestData();
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertDateRangeTestData();
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertDateRangeTestData();
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertNumericRangeTestData();
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertNumericRangeTestData();
            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertNumericRangeTestData();
            int blockCountLimit = 1;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertNumericRangeTestData();
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertNumericRangeTestData();
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
            InsertNumericRangeTestData();
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = new FindDeadBlocksRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                1,
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
