using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tasks;
using Taskling.Tasks;
using System.Threading.Tasks;
using System.Threading;

namespace Taskling.SqlServer.Tests.Repositories.Given_BlockRepository
{
    public class When_FindDeadBlocks
    {
        private int _taskDefinitionId;
        private string _taskExecution1;
        private string _block1;
        private string _block2;
        private string _block3;
        private string _block4;
        private string _block5;

        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        private readonly TimeSpan FiveMinuteSpan = new TimeSpan(0, 5, 0);
        private readonly TimeSpan OneMinuteSpan = new TimeSpan(0, 1, 0);
        private readonly TimeSpan TwentySecondSpan = new TimeSpan(0, 0, 20);

        public When_FindDeadBlocks()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertAvailableExecutionToken(_taskDefinitionId);

            TaskRepository.ClearCache();
        }
        
        private void InsertDateRangeTestData(TaskDeathMode taskDeathMode)
        {
            var now = DateTime.UtcNow;
            if (taskDeathMode == TaskDeathMode.Override)
                _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));
            else
                _taskExecution1 = _executionHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

            InsertDateRangeBlocksTestData();
        }

        private void InsertDateRangeBlocksTestData()
        {
            var now = DateTime.UtcNow;
            _block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-180), now.AddMinutes(-179)).ToString();
            _block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-200), now.AddMinutes(-199)).ToString();
            _block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-220), now.AddMinutes(-219)).ToString();
            _block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-240), now.AddMinutes(-239)).ToString();
            _block5 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, now.AddMinutes(-250), now.AddMinutes(-249)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), now.AddMinutes(-180), now.AddMinutes(-180), now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), now.AddMinutes(-200), now.AddMinutes(-200), null, BlockExecutionStatus.Started, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), now.AddMinutes(-220), null, null, BlockExecutionStatus.NotStarted, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), now.AddMinutes(-240), now.AddMinutes(-240), now.AddMinutes(-235), BlockExecutionStatus.Completed, 2);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), now.AddMinutes(-250), now.AddMinutes(-250), null, BlockExecutionStatus.Started, 3);
        }

        private void InsertNumericRangeTestData(TaskDeathMode taskDeathMode)
        {
            var now = DateTime.UtcNow;
            if (taskDeathMode == TaskDeathMode.Override)
                _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));
            else
                _taskExecution1 = _executionHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

            InsertNumericRangeBlocksTestData();
        }

        private void InsertNumericRangeBlocksTestData()
        {
            var now = DateTime.UtcNow;
            _block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, now.AddMinutes(-100)).ToString();
            _block2 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 101, 200, now.AddMinutes(-90)).ToString();
            _block3 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 201, 300, now.AddMinutes(-80)).ToString();
            _block4 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 301, 400, now.AddMinutes(-70)).ToString();
            _block5 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 401, 500, now.AddMinutes(-60)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), now.AddMinutes(-180), now.AddMinutes(-180), now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), now.AddMinutes(-200), now.AddMinutes(-200), null, BlockExecutionStatus.Started, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), now.AddMinutes(-220), null, null, BlockExecutionStatus.NotStarted, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), now.AddMinutes(-240), now.AddMinutes(-240), now.AddMinutes(-235), BlockExecutionStatus.Completed, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), now.AddMinutes(-250), now.AddMinutes(-250), null, BlockExecutionStatus.Started, 3);
        }

        private void InsertListTestData(TaskDeathMode taskDeathMode)
        {
            var now = DateTime.UtcNow;
            if (taskDeathMode == TaskDeathMode.Override)
                _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));
            else
                _taskExecution1 = _executionHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

            InsertListBlocksTestData();
        }

        private void InsertListBlocksTestData()
        {
            var now = DateTime.UtcNow;
            _block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-246)).ToString();
            _block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-247)).ToString();
            _block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-248)).ToString();
            _block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-249)).ToString();
            _block5 = _blocksHelper.InsertListBlock(_taskDefinitionId, now.AddMinutes(-250)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), now.AddMinutes(-180), now.AddMinutes(-180), now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), now.AddMinutes(-200), now.AddMinutes(-200), null, BlockExecutionStatus.Started, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), now.AddMinutes(-220), null, null, BlockExecutionStatus.NotStarted, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), now.AddMinutes(-240), now.AddMinutes(-240), now.AddMinutes(-235), BlockExecutionStatus.Completed, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), now.AddMinutes(-250), now.AddMinutes(-250), null, BlockExecutionStatus.Started, 3);
        }

        private void InsertObjectTestData(TaskDeathMode taskDeathMode)
        {
            var now = DateTime.UtcNow;
            if (taskDeathMode == TaskDeathMode.Override)
                _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId, OneMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));
            else
                _taskExecution1 = _executionHelper.InsertKeepAliveTaskExecution(_taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.AddMinutes(-250), now.AddMinutes(-179));

            InsertObjectBlocksTestData();
        }

        private void InsertObjectBlocksTestData()
        {
            var now = DateTime.UtcNow;
            _block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-246), Guid.NewGuid().ToString()).ToString();
            _block2 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-247), Guid.NewGuid().ToString()).ToString();
            _block3 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-248), Guid.NewGuid().ToString()).ToString();
            _block4 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-249), Guid.NewGuid().ToString()).ToString();
            _block5 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, now.AddMinutes(-250), Guid.NewGuid().ToString()).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), now.AddMinutes(-180), now.AddMinutes(-180), now.AddMinutes(-175), BlockExecutionStatus.Failed, 2);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), now.AddMinutes(-200), now.AddMinutes(-200), null, BlockExecutionStatus.Started, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), now.AddMinutes(-220), null, null, BlockExecutionStatus.NotStarted, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), now.AddMinutes(-240), now.AddMinutes(-240), now.AddMinutes(-235), BlockExecutionStatus.Completed, 1);
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), now.AddMinutes(-250), now.AddMinutes(-250), null, BlockExecutionStatus.Started, 3);
        }

        private BlockRepository CreateSut()
        {
            return new BlockRepository(new TaskRepository());
        }

        #region .: Date Range Blocks :.

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndDateRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndDateRange_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            int retryLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit, retryLimit, -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndDateRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertDateRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        #endregion .: Date Range Blocks :.

        #region .: Numeric Range Blocks :.

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndNumericRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndNumericRange_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            int retryLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit, retryLimit, -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.RangeBlockId == _block3);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndNumericRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertNumericRangeTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadRangeBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        #endregion .: Date Range Blocks :.

        #region .: List Blocks :.

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndList_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndList_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndList_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            int blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndList_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            int attemptLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit, attemptLimit, -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ListBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndList_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertListTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadListBlocksAsync(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        #endregion .: List Blocks :.

        #region .: Object Blocks :.

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndObject_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndObject_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndObject_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            int blockCountLimit = 1;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(1, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_OverrideModeAndObject_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.Override);
            int blockCountLimit = 5;
            int attemptLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit, attemptLimit, -300);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(3, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block2);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-250));

            int blockCountLimit = 2;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(2, deadBlocks.Count);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block3);
            Assert.Contains(deadBlocks, x => x.ObjectBlockId == _block5);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndObject_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-2));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public async Task When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
        {
            // ARRANGE
            InsertObjectTestData(TaskDeathMode.KeepAlive);
            _executionHelper.SetKeepAlive(_taskExecution1, DateTime.UtcNow.AddMinutes(-50));

            int blockCountLimit = 5;
            var request = CreateDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit, 3, -100);

            // ACT
            var sut = CreateSut();
            var deadBlocks = await sut.FindDeadObjectBlocksAsync<string>(request);

            // ASSERT
            Assert.Equal(0, deadBlocks.Count);
        }

        #endregion .: Object Blocks :.

        private FindDeadBlocksRequest CreateDeadBlockRequest(BlockType blockType, TaskDeathMode taskDeathMode, int blockCountLimit)
        {
            return CreateDeadBlockRequest(blockType, taskDeathMode, blockCountLimit, 3, -300);
        }

        private FindDeadBlocksRequest CreateDeadBlockRequest(BlockType blockType, TaskDeathMode taskDeathMode, int blockCountLimit, int attemptLimit, int fromMinutesBack)
        {
            return new FindDeadBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                blockType,
                DateTime.UtcNow.AddMinutes(fromMinutesBack),
                DateTime.UtcNow,
                blockCountLimit,
                taskDeathMode,
                attemptLimit);
        }
    }
}
