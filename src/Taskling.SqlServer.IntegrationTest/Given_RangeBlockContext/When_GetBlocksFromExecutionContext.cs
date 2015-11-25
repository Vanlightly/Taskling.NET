using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.IntegrationTest.Given_RangeBlockContext
{
    [TestClass]
    public class When_GetBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, TestConstants.TaskName);

            var taskSecondaryId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertUnlimitedExecutionToken(taskSecondaryId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };

            // ACT and // ASSERT
            bool startedOk;
            var client = new SqlServerTasklingClient(settings);
            using (var executionContext = client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = executionContext.GetRangeBlocks(x => x.AsDateRange(fromDate, toDate, maxBlockRange)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10));
                    Assert.AreEqual(10, _blocksHelper.GetDateRangeBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 10;
                    int expectedCompletedCount = 0;
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetDateRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetDateRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetDateRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        expectedNotStartedCount--;
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetDateRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetDateRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        

                        // processing here
                        rangeBlock.Complete();
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetDateRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };

            // ACT and // ASSERT
            bool startedOk;
            var client = new SqlServerTasklingClient(settings);
            using (var executionContext = client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = executionContext.GetRangeBlocks(x => x.AsNumericRange(fromNumber, toNumber, maxBlockRange)
                                                                .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0))
                                                                .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 1, 0, 0))
                                                                .MaximumBlocksToGenerate(10));
                    Assert.AreEqual(10, _blocksHelper.GetNumericRangeBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 10;
                    int expectedCompletedCount = 0;
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetNumericRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetNumericRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetNumericRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        expectedNotStartedCount--;
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetNumericRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetNumericRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        // processing here
                       

                        block.Complete();
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetNumericRangeBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }
    }
}
