using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_RangeBlockContext
{
    [TestClass]
    public class When_GetRangeBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;
        private int _taskDefinitionId;

        [TestInitialize]
        public void Initialize()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            int blockCountLimit = 10;

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(blockCountLimit))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(fromDate, toDate, maxBlockRange));
                    Assert.AreEqual(10, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = blockCountLimit;
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
        public void If_AsDateRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow;
                    var toDate = DateTime.UtcNow.AddHours(-12);
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(fromDate, toDate, maxBlockRange));
                    Assert.AreEqual(0, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

                    var lastEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.AreEqual(EventType.CheckPoint, lastEvent.Item1);
                    Assert.AreEqual("No values for generate the block. Emtpy Block context returned.", lastEvent.Item2);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 800;
                    var maxBlockRange = 100;
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(fromNumber, toNumber, maxBlockRange));
                    Assert.AreEqual(0, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

                    var lastEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.AreEqual(EventType.CheckPoint, lastEvent.Item1);
                    Assert.AreEqual("No values for generate the block. Emtpy Block context returned.", lastEvent.Item2);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            int blockCountLimit = 10;

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(blockCountLimit))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(fromNumber, toNumber, maxBlockRange));
                    Assert.AreEqual(10, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = blockCountLimit;
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
        public void If_AsNumericRange_BlocksDoNotShareIds()
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromNumber = 0;
                    var toNumber = 100;
                    var maxBlockRange = 10;
                    var blocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(fromNumber, toNumber, maxBlockRange));

                    int counter = 0;
                    INumericRangeBlockContext lastBlock = null;
                    foreach (var block in blocks)
                    {
                        if (counter > 0)
                            Assert.AreEqual(lastBlock.NumericRangeBlock.EndNumber + 1, block.NumericRangeBlock.StartNumber);

                        lastBlock = block;
                        counter++;
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
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(new DateTime(2016, 1, 1), new DateTime(2016, 1, 31, 23, 59, 59, 999).AddMilliseconds(-1), new TimeSpan(1, 0, 0, 0))
                        .OverrideConfiguration()
                        .MaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            IDateRangeBlock expectedLastBlock = new RangeBlock("0", 1, new DateTime(2016, 1, 31).Ticks, new DateTime(2016, 1, 31, 23, 59, 59, 997).Ticks, BlockType.DateRange);

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastDateRangeBlock(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.StartDate, lastBlock.StartDate);
            Assert.AreEqual(expectedLastBlock.EndDate, lastBlock.EndDate);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastDateRangeBlock(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.IsNull(lastBlock);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(new DateTime(2016, 1, 1), new DateTime(2016, 1, 2), new TimeSpan(2, 0, 0, 0))
                        .OverrideConfiguration()
                        .MaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            _blocksHelper.InsertPhantomDateRangeBlock(TestConstants.ApplicationName, TestConstants.TaskName, new DateTime(2015, 1, 1), new DateTime(2015, 1, 2));

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastDateRangeBlock(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.AreEqual(new DateTime(2016, 1, 1), lastBlock.StartDate);
            Assert.AreEqual(new DateTime(2016, 1, 2), lastBlock.EndDate);
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
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(1, 1000, 100));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            var expectedLastBlock = new RangeBlock("0", 1, 901, 1000, BlockType.NumericRange);

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastNumericRangeBlock(LastBlockOrder.MaxRangeStartValue);
                }
            }

            // ASSERT
            Assert.AreEqual(expectedLastBlock.RangeBeginAsInt(), lastBlock.StartNumber);
            Assert.AreEqual(expectedLastBlock.RangeEndAsInt(), lastBlock.EndNumber);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastNumericRangeBlock(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.IsNull(lastBlock);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(1000, 2000, 2000));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            _blocksHelper.InsertPhantomNumericBlock(TestConstants.ApplicationName, TestConstants.TaskName, 0, 100);

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    lastBlock = executionContext.GetLastNumericRangeBlock(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.AreEqual(1000, (int)lastBlock.StartNumber);
            Assert.AreEqual(2000, (int)lastBlock.EndNumber);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart(referenceValue);
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(fromDate, toDate, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    rangeBlocks[0].Start();
                    rangeBlocks[0].Complete(); // completed
                    rangeBlocks[1].Start();
                    rangeBlocks[1].Failed("Something bad happened"); // failed
                    // 2 not started
                    rangeBlocks[3].Start(); // started
                    rangeBlocks[4].Start();
                    rangeBlocks[4].Complete(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.ReprocessDateRange()
                                                        .PendingAndFailedBlocks()
                                                        .OfExecutionWith(referenceValue)).ToList();

                    Assert.AreEqual(3, rangeBlocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart(referenceValue);
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(fromDate, toDate, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    rangeBlocks[0].Start();
                    rangeBlocks[0].Complete(); // completed
                    rangeBlocks[1].Start();
                    rangeBlocks[1].Failed(); // failed
                    // 2 not started
                    rangeBlocks[3].Start(); // started
                    rangeBlocks[4].Start();
                    rangeBlocks[4].Complete(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.ReprocessDateRange()
                                                        .AllBlocks()
                                                        .OfExecutionWith(referenceValue)).ToList();

                    Assert.AreEqual(5, rangeBlocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart(referenceValue);
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(fromNumber, toNumber, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    blocks[0].Start();
                    blocks[0].Complete(); // completed
                    blocks[1].Start();
                    blocks[1].Failed(); // failed
                    // 2 not started
                    blocks[3].Start(); // started
                    blocks[4].Start();
                    blocks[4].Complete(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.ReprocessNumericRange()
                                                        .PendingAndFailedBlocks()
                                                        .OfExecutionWith(referenceValue)).ToList();

                    Assert.AreEqual(3, rangeBlocks.Count);
                }
            }
        }


        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart(referenceValue);
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(fromNumber, toNumber, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    blocks[0].Start();
                    blocks[0].Complete(); // completed
                    blocks[1].Start();
                    blocks[1].Failed(); // failed
                    // 2 not started
                    blocks[3].Start(); // started
                    blocks[4].Start();
                    blocks[4].Complete(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.ReprocessNumericRange()
                                                        .AllBlocks()
                                                        .OfExecutionWith(referenceValue)).ToList();

                    Assert.AreEqual(5, rangeBlocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRangeWithPreviousDeadBlocks_ThenReprocessOk()
        {
            // ARRANGE
            CreateFailedDateTask();
            CreateDeadDateTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 7);
                    var to = new DateTime(2016, 1, 7);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    int counter = 0;
                    foreach (var block in dateBlocks)
                    {
                        block.Start();

                        block.Complete();

                        counter++;
                        Assert.AreEqual(counter, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }


                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            CreateFailedDateTask();
            CreateDeadDateTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 7);
                    var to = new DateTime(2016, 1, 31);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    Assert.AreEqual(8, dateBlocks.Count());
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 1)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 2)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 3)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 4)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 5)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 6)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 7)));
                    Assert.IsTrue(dateBlocks.Any(x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 8)));
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            int blockCountLimit = 10;
            CreateFailedDateTask();
            CreateDeadDateTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing(blockCountLimit))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 7);
                    var to = new DateTime(2016, 1, 31);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var numericBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(from, to, maxBlockSize));
                    Assert.AreEqual(10, numericBlocks.Count());
                    Assert.IsTrue(numericBlocks.All(x => x.DateRangeBlock.StartDate >= from));
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            CreateFailedNumericTask();
            CreateDeadNumericTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    long from = 61;
                    long to = 200;
                    short maxBlockSize = 10;
                    var numericBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    Assert.AreEqual(8, numericBlocks.Count());
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 1));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 11));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 21));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 31));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 41));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 51));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber == 61));
                    Assert.IsTrue(numericBlocks.Any(x => (int)x.NumericRangeBlock.StartNumber
                    == 71));
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            int blockCountLimit = 10;
            CreateFailedNumericTask();
            CreateDeadNumericTask();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing(blockCountLimit))
            {
                startedOk = executionContext.TryStart();
                Assert.AreEqual(true, startedOk);
                if (startedOk)
                {
                    long from = 61;
                    long to = 200;
                    short maxBlockSize = 10;
                    var numericBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(from, to, maxBlockSize));
                    Assert.AreEqual(10, numericBlocks.Count());
                    Assert.IsTrue(numericBlocks.All(x => (int)x.NumericRangeBlock.StartNumber >= 61));
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsDateRange_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            var fromDate = DateTime.UtcNow.AddHours(-12);
            var toDate = DateTime.UtcNow;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var maxBlockRange = new TimeSpan(24, 0, 0);
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(fromDate, toDate, maxBlockRange));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.OnlyOldDateBlocks());
                    Assert.AreEqual(1, rangeBlocks.Count);
                    Assert.AreEqual(fromDate.ToString("yyyyMMdd HH:mm:ss"), rangeBlocks[0].DateRangeBlock.StartDate.ToString("yyyyMMdd HH:mm:ss"));
                    Assert.AreEqual(toDate.ToString("yyyyMMdd HH:mm:ss"), rangeBlocks[0].DateRangeBlock.EndDate.ToString("yyyyMMdd HH:mm:ss"));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetDateRangeBlocks(x => x.OnlyOldDateBlocks());
                    Assert.AreEqual(0, rangeBlocks.Count);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_AsNumericRange_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            long fromNumber = 1000;
            long toNumber = 2000;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var maxBlockRange = 2000;
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(fromNumber, toNumber, maxBlockRange));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.OnlyOldNumericBlocks());
                    Assert.AreEqual(1, rangeBlocks.Count);
                    Assert.AreEqual(fromNumber, rangeBlocks[0].NumericRangeBlock.StartNumber);
                    Assert.AreEqual(toNumber, rangeBlocks[0].NumericRangeBlock.EndNumber);
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        rangeBlock.Start();
                        rangeBlock.Complete();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var rangeBlocks = executionContext.GetNumericRangeBlocks(x => x.OnlyOldNumericBlocks());
                    Assert.AreEqual(0, rangeBlocks.Count);
                }
            }
        }

        private ITaskExecutionContext CreateTaskExecutionContext(int maxBlocksToCreate = 2000)
        {
            return ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToCreate));
        }

        private ITaskExecutionContext CreateTaskExecutionContextWithNoReprocessing(int maxBlocksToCreate = 2000)
        {
            return ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(maxBlocksToCreate));
        }

        private void CreateFailedDateTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 1);
                    var to = new DateTime(2016, 1, 4);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in dateBlocks)
                    {
                        block.Start();
                        block.Failed();
                    }
                }
            }
        }

        private void CreateDeadDateTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 4);
                    var to = new DateTime(2016, 1, 7);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = executionContext.GetDateRangeBlocks(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in dateBlocks)
                    {
                        block.Start();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }

        private void CreateFailedNumericTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    long from = 1;
                    long to = 30;
                    short maxBlockSize = 10;
                    var numericBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in numericBlocks)
                    {
                        block.Start();
                        block.Failed();
                    }
                }
            }
        }

        private void CreateDeadNumericTask()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = executionContext.TryStart();
                if (startedOk)
                {
                    long from = 31;
                    long to = 60;
                    short maxBlockSize = 10;
                    var numericBlocks = executionContext.GetNumericRangeBlocks(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in numericBlocks)
                    {
                        block.Start();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }
    }
}
