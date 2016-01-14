using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.Blocks;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.IntegrationTest.Given_RangeBlockContext
{
    [TestClass]
    public class When_GetRangeBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            var taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertExecutionToken(taskDefinitionId, TaskExecutionStatus.Available, "0");
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0)
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveDeathThreshold = new TimeSpan(0, 0, 2, 0)
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
                    Assert.AreEqual(10, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 10;
                    int expectedCompletedCount = 0;
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        expectedNotStartedCount--;
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        

                        // processing here
                        rangeBlock.Complete();
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
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
                ConnectTimeout = new TimeSpan(0, 0, 1, 0)
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveDeathThreshold = new TimeSpan(0, 0, 2, 0)
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
                    Assert.AreEqual(10, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = 10;
                    int expectedCompletedCount = 0;
                    Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.AreEqual(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var block in blocks)
                    {
                        block.Start();
                        expectedNotStartedCount--;
                        Assert.AreEqual(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.AreEqual(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        // processing here
                       

                        block.Complete();
                        expectedCompletedCount++;
                        Assert.AreEqual(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_PreviousBlock_ThenLastBlockContainsDates()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetRangeBlocks(x => x.AsDateRange(new DateTime(2016, 1, 1), new DateTime(2016, 1, 31), new TimeSpan(1, 0, 0, 0))
                                                                .MaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            var expectedLastBlock = new RangeBlock("0", new DateTime(2016, 1, 30).Ticks, new DateTime(2016, 1, 31).Ticks);
            
            // ACT
            RangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastDateRangeBlock();
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.RangeBeginAsDateTime(), lastBlock.RangeBeginAsDateTime());
            Assert.AreEqual(expectedLastBlock.RangeEndAsDateTime(), lastBlock.RangeEndAsDateTime());
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_NoPreviousBlock_ThenLastBlockIsEmpty()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            RangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastDateRangeBlock();
                }
            }

            // ASSERT
            Assert.AreEqual("0", lastBlock.RangeBlockId);
            Assert.AreEqual(0, lastBlock.RangeBeginAsInt());
            Assert.AreEqual(0, lastBlock.RangeEndAsInt());
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_PreviousBlock_ThenLastBlockContainsDates()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetRangeBlocks(x => x.AsNumericRange(1, 1000, 100)
                                                                .MaximumBlocksToGenerate(10));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            var expectedLastBlock = new RangeBlock("0", 901, 1000);

            // ACT
            RangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastNumericRangeBlock();
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.RangeBeginAsInt(), lastBlock.RangeBeginAsInt());
            Assert.AreEqual(expectedLastBlock.RangeEndAsInt(), lastBlock.RangeEndAsInt());
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_NoPreviousBlock_ThenLastBlockIsEmpty()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            RangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastNumericRangeBlock();
                }
            }

            // ASSERT
            Assert.AreEqual("0", lastBlock.RangeBlockId);
            Assert.AreEqual(0, lastBlock.RangeBeginAsInt());
            Assert.AreEqual(0, lastBlock.RangeEndAsInt());
        }

        private ITaskExecutionContext CreateTaskExecutionContext()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0)
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveDeathThreshold = new TimeSpan(0, 0, 2, 0)
            };

            var client = new SqlServerTasklingClient(settings);
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions);
        }
    }
}
