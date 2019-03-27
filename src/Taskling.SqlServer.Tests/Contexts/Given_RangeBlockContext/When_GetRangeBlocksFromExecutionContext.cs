using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.RangeBlocks;
using Taskling.Contexts;
using Taskling.Events;
using Taskling.SqlServer.Tests.Helpers;
using System.Threading.Tasks;
using System.Threading;

namespace Taskling.SqlServer.Tests.Contexts.Given_RangeBlockContext
{
    public class When_GetRangeBlocksFromExecutionContext
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;
        private int _taskDefinitionId;

        public When_GetRangeBlocksFromExecutionContext()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);
        }
        
        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            int blockCountLimit = 10;

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(fromDate, toDate, maxBlockRange));
                    Assert.Equal(10, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = blockCountLimit;
                    int expectedCompletedCount = 0;
                    Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        expectedNotStartedCount--;
                        Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));



                        // processing here
                        await rangeBlock.CompleteAsync();
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow;
                    var toDate = DateTime.UtcNow.AddHours(-12);
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(fromDate, toDate, maxBlockRange));
                    Assert.Equal(0, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

                    var lastEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventType.CheckPoint, lastEvent.Item1);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", lastEvent.Item2);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 800;
                    var maxBlockRange = 100;
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(fromNumber, toNumber, maxBlockRange));
                    Assert.Equal(0, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

                    var lastEvent = _executionHelper.GetLastEvent(_taskDefinitionId);
                    Assert.Equal(EventType.CheckPoint, lastEvent.Item1);
                    Assert.Equal("No values for generate the block. Emtpy Block context returned.", lastEvent.Item2);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
        {
            // ARRANGE
            int blockCountLimit = 10;

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(fromNumber, toNumber, maxBlockRange));
                    Assert.Equal(10, _blocksHelper.GetBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
                    int expectedNotStartedCount = blockCountLimit;
                    int expectedCompletedCount = 0;
                    Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                    Assert.Equal(0, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
                    Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

                    foreach (var block in blocks)
                    {
                        await block.StartAsync();
                        expectedNotStartedCount--;
                        Assert.Equal(expectedNotStartedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                        Assert.Equal(1, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                        // processing here


                        await block.CompleteAsync();
                        expectedCompletedCount++;
                        Assert.Equal(expectedCompletedCount, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_BlocksDoNotShareIds()
        {
            // ARRANGE
            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 0;
                    var toNumber = 100;
                    var maxBlockRange = 10;
                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(fromNumber, toNumber, maxBlockRange));

                    int counter = 0;
                    INumericRangeBlockContext lastBlock = null;
                    foreach (var block in blocks)
                    {
                        if (counter > 0)
                            Assert.Equal(lastBlock.NumericRangeBlock.EndNumber + 1, block.NumericRangeBlock.StartNumber);

                        lastBlock = block;
                        counter++;
                    }
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_PreviousBlock_ThenLastBlockContainsDates()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(new DateTime(2016, 1, 1), new DateTime(2016, 1, 31, 23, 59, 59, 999).AddMilliseconds(-1), new TimeSpan(1, 0, 0, 0))
                        .OverrideConfiguration()
                        .MaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            IDateRangeBlock expectedLastBlock = new RangeBlock("0", 1, new DateTime(2016, 1, 31).Ticks, new DateTime(2016, 1, 31, 23, 59, 59, 997).Ticks, BlockType.DateRange);

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastDateRangeBlockAsync(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.Equal(expectedLastBlock.StartDate, lastBlock.StartDate);
            Assert.Equal(expectedLastBlock.EndDate, lastBlock.EndDate);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastDateRangeBlockAsync(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.Null(lastBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(new DateTime(2016, 1, 1), new DateTime(2016, 1, 2), new TimeSpan(2, 0, 0, 0))
                        .OverrideConfiguration()
                        .MaximumBlocksToGenerate(50));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomDateRangeBlock(TestConstants.ApplicationName, TestConstants.TaskName, new DateTime(2015, 1, 1), new DateTime(2015, 1, 2));

            // ACT
            IDateRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastDateRangeBlockAsync(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.Equal(new DateTime(2016, 1, 1), lastBlock.StartDate);
            Assert.Equal(new DateTime(2016, 1, 2), lastBlock.EndDate);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_PreviousBlock_ThenLastBlockContainsDates()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1, 1000, 100));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            var expectedLastBlock = new RangeBlock("0", 1, 901, 1000, BlockType.NumericRange);

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastNumericRangeBlockAsync(LastBlockOrder.MaxRangeStartValue);
                }
            }

            // ASSERT
            Assert.Equal(expectedLastBlock.RangeBeginAsInt(), lastBlock.StartNumber);
            Assert.Equal(expectedLastBlock.RangeEndAsInt(), lastBlock.EndNumber);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_NoPreviousBlock_ThenLastBlockIsNull()
        {
            // ARRANGE
            // all previous blocks were deleted in TestInitialize

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastNumericRangeBlockAsync(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.Null(lastBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
        {
            // ARRANGE
            // Create previous blocks
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(1000, 2000, 2000));

                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            _blocksHelper.InsertPhantomNumericBlock(TestConstants.ApplicationName, TestConstants.TaskName, 0, 100);

            // ACT
            INumericRangeBlock lastBlock = null;
            using (var executionContext = CreateTaskExecutionContext())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    lastBlock = await executionContext.GetLastNumericRangeBlockAsync(LastBlockOrder.LastCreated);
                }
            }

            // ASSERT
            Assert.Equal(1000, (int)lastBlock.StartNumber);
            Assert.Equal(2000, (int)lastBlock.EndNumber);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(fromDate, toDate, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    await rangeBlocks[0].StartAsync();
                    await rangeBlocks[0].CompleteAsync(); // completed
                    await rangeBlocks[1].StartAsync();
                    await rangeBlocks[1].FailedAsync("Something bad happened"); // failed
                    // 2 not started
                    await rangeBlocks[3].StartAsync(); // started
                    await rangeBlocks[4].StartAsync();
                    await rangeBlocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.ReprocessDateRange()
                                                        .PendingAndFailedBlocks()
                                                        .OfExecutionWith(referenceValue));

                    Assert.Equal(3, rangeBlocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromDate = DateTime.UtcNow.AddHours(-12);
                    var toDate = DateTime.UtcNow;
                    var maxBlockRange = new TimeSpan(0, 30, 0);
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(fromDate, toDate, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    await rangeBlocks[0].StartAsync();
                    await rangeBlocks[0].CompleteAsync(); // completed
                    await rangeBlocks[1].StartAsync();
                    await rangeBlocks[1].FailedAsync(); // failed
                    // 2 not started
                    await rangeBlocks[3].StartAsync(); // started
                    await rangeBlocks[4].StartAsync();
                    await rangeBlocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.ReprocessDateRange()
                                                        .AllBlocks()
                                                        .OfExecutionWith(referenceValue));

                    Assert.Equal(5, rangeBlocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(fromNumber, toNumber, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync(); // failed
                    // 2 not started
                    await blocks[3].StartAsync(); // started
                    await blocks[4].StartAsync();
                    await blocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.ReprocessNumericRange()
                                                        .PendingAndFailedBlocks()
                                                        .OfExecutionWith(referenceValue));

                    Assert.Equal(3, rangeBlocks.Count);
                }
            }
        }


        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
        {
            // ARRANGE
            var referenceValue = Guid.NewGuid().ToString();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync(referenceValue);
                Assert.True(startedOk);
                if (startedOk)
                {
                    var fromNumber = 1000;
                    var toNumber = 3000;
                    var maxBlockRange = 100;
                    var blocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(fromNumber, toNumber, maxBlockRange)
                                                                .OverrideConfiguration()
                                                                .MaximumBlocksToGenerate(5));

                    await blocks[0].StartAsync();
                    await blocks[0].CompleteAsync(); // completed
                    await blocks[1].StartAsync();
                    await blocks[1].FailedAsync(); // failed
                    // 2 not started
                    await blocks[3].StartAsync(); // started
                    await blocks[4].StartAsync();
                    await blocks[4].CompleteAsync(); // completed
                }
            }

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.ReprocessNumericRange()
                                                        .AllBlocks()
                                                        .OfExecutionWith(referenceValue));

                    Assert.Equal(5, rangeBlocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRangeWithPreviousDeadBlocks_ThenReprocessOk()
        {
            // ARRANGE
            await CreateFailedDateTaskAsync();
            await CreateDeadDateTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 7);
                    var to = new DateTime(2016, 1, 7);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    int counter = 0;
                    foreach (var block in dateBlocks)
                    {
                        await block.StartAsync();

                        await block.CompleteAsync();

                        counter++;
                        Assert.Equal(counter, _blocksHelper.GetBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
                    }


                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            await CreateFailedDateTaskAsync();
            await CreateDeadDateTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 7);
                    var to = new DateTime(2016, 1, 31);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    Assert.Equal(8, dateBlocks.Count());
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 1));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 2));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 3));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 4));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 5));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 6));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 7));
                    Assert.Contains(dateBlocks, x => x.DateRangeBlock.StartDate == new DateTime(2016, 1, 8));
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            int blockCountLimit = 10;
            await CreateFailedDateTaskAsync();
            await CreateDeadDateTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 7);
                    var to = new DateTime(2016, 1, 31);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var numericBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));
                    Assert.Equal(10, numericBlocks.Count());
                    Assert.True(numericBlocks.All(x => x.DateRangeBlock.StartDate >= from));
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
        {
            // ARRANGE
            await CreateFailedNumericTaskAsync();
            await CreateDeadNumericTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    long from = 61;
                    long to = 200;
                    short maxBlockSize = 10;
                    var numericBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize)
                        .OverrideConfiguration()
                        .ReprocessDeadTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .ReprocessFailedTasks(new TimeSpan(1, 0, 0, 0), 3)
                        .MaximumBlocksToGenerate(8));

                    Assert.Equal(8, numericBlocks.Count());
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 1);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 11);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 21);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 31);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 41);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 51);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 61);
                    Assert.Contains(numericBlocks, x => (int)x.NumericRangeBlock.StartNumber == 71);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
        {
            // ARRANGE
            int blockCountLimit = 10;
            await CreateFailedNumericTaskAsync();
            await CreateDeadNumericTaskAsync();

            // ACT and // ASSERT
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing(blockCountLimit))
            {
                startedOk = await executionContext.TryStartAsync();
                Assert.True(startedOk);
                if (startedOk)
                {
                    long from = 61;
                    long to = 200;
                    short maxBlockSize = 10;
                    var numericBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));
                    Assert.Equal(10, numericBlocks.Count());
                    Assert.True(numericBlocks.All(x => (int)x.NumericRangeBlock.StartNumber >= 61));
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsDateRange_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            var fromDate = DateTime.UtcNow.AddHours(-12);
            var toDate = DateTime.UtcNow;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var maxBlockRange = new TimeSpan(24, 0, 0);
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(fromDate, toDate, maxBlockRange));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.OnlyOldDateBlocks());
                    Assert.Equal(1, rangeBlocks.Count);
                    Assert.Equal(fromDate.ToString("yyyyMMdd HH:mm:ss"), rangeBlocks[0].DateRangeBlock.StartDate.ToString("yyyyMMdd HH:mm:ss"));
                    Assert.Equal(toDate.ToString("yyyyMMdd HH:mm:ss"), rangeBlocks[0].DateRangeBlock.EndDate.ToString("yyyyMMdd HH:mm:ss"));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.OnlyOldDateBlocks());
                    Assert.Equal(0, rangeBlocks.Count);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task If_AsNumericRange_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
        {
            // ARRANGE
            long fromNumber = 1000;
            long toNumber = 2000;

            // create a block
            bool startedOk;
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var maxBlockRange = 2000;
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(fromNumber, toNumber, maxBlockRange));
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // add this processed block to the forced queue
            var lastBlockId = _blocksHelper.GetLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
            _blocksHelper.EnqueueForcedBlock(lastBlockId);

            // ACT - reprocess the forced block
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.OnlyOldNumericBlocks());
                    Assert.Equal(1, rangeBlocks.Count);
                    Assert.Equal(fromNumber, rangeBlocks[0].NumericRangeBlock.StartNumber);
                    Assert.Equal(toNumber, rangeBlocks[0].NumericRangeBlock.EndNumber);
                    foreach (var rangeBlock in rangeBlocks)
                    {
                        await rangeBlock.StartAsync();
                        await rangeBlock.CompleteAsync();
                    }
                }
            }

            // The forced block will have been dequeued so it should not be processed again
            using (var executionContext = CreateTaskExecutionContext())
            {
                startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var rangeBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.OnlyOldNumericBlocks());
                    Assert.Equal(0, rangeBlocks.Count);
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

        private async Task CreateFailedDateTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 1);
                    var to = new DateTime(2016, 1, 4);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in dateBlocks)
                    {
                        await block.StartAsync();
                        await block.FailedAsync();
                    }
                }
            }
        }

        private async Task CreateDeadDateTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    var from = new DateTime(2016, 1, 4);
                    var to = new DateTime(2016, 1, 7);
                    var maxBlockSize = new TimeSpan(1, 0, 0, 0);
                    var dateBlocks = await executionContext.GetDateRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in dateBlocks)
                    {
                        await block.StartAsync();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }

        private async Task CreateFailedNumericTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    long from = 1;
                    long to = 30;
                    short maxBlockSize = 10;
                    var numericBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in numericBlocks)
                    {
                        await block.StartAsync();
                        await block.FailedAsync();
                    }
                }
            }
        }

        private async Task CreateDeadNumericTaskAsync()
        {
            using (var executionContext = CreateTaskExecutionContextWithNoReprocessing())
            {
                var startedOk = await executionContext.TryStartAsync();
                if (startedOk)
                {
                    long from = 31;
                    long to = 60;
                    short maxBlockSize = 10;
                    var numericBlocks = await executionContext.GetNumericRangeBlocksAsync(x => x.WithRange(from, to, maxBlockSize));

                    foreach (var block in numericBlocks)
                    {
                        await block.StartAsync();
                    }
                }
            }

            var executionHelper = new ExecutionsHelper();
            executionHelper.SetLastExecutionAsDead(_taskDefinitionId);
        }
    }
}
