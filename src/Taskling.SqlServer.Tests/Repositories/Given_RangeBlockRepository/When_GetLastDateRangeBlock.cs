using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.Tests.Repositories.Given_RangeBlockRepository
{
    public class When_GetLastDateRangeBlock
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        private int _taskDefinitionId;
        private string _taskExecution1;
        private DateTime _baseDateTime;

        private string _block1;
        private string _block2;
        private string _block3;
        private string _block4;
        private string _block5;

        public When_GetLastDateRangeBlock()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

            TaskRepository.ClearCache();
        }

        private RangeBlockRepository CreateSut()
        {
            return new RangeBlockRepository(new TaskRepository());
        }

        private void InsertBlocks()
        {
            _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

            _baseDateTime = new DateTime(2016, 1, 1);
            _block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-30), DateTime.UtcNow).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Failed);
            Thread.Sleep(10);
            _block2 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-10), _baseDateTime.AddMinutes(-40), DateTime.UtcNow).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatus.Started);
            Thread.Sleep(10);
            _block3 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-50), DateTime.UtcNow).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatus.NotStarted);
            Thread.Sleep(10);
            _block4 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-60), DateTime.UtcNow).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatus.Completed);
            Thread.Sleep(10);
            _block5 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-70), DateTime.UtcNow).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatus.Started);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public void If_OrderByLastCreated_ThenReturnLastCreated()
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = sut.GetLastRangeBlock(CreateRequest(LastBlockOrder.LastCreated));

            // ASSERT
            Assert.Equal(_block5, block.RangeBlockId);
            Assert.Equal(_baseDateTime.AddMinutes(-60), block.RangeBeginAsDateTime());
            Assert.Equal(_baseDateTime.AddMinutes(-70), block.RangeEndAsDateTime());
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public void If_OrderByMaxFromDate_ThenReturnBlockWithMaxFromDate()
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = sut.GetLastRangeBlock(CreateRequest(LastBlockOrder.MaxRangeStartValue));

            // ASSERT
            Assert.Equal(_block2, block.RangeBlockId);
            Assert.Equal(_baseDateTime.AddMinutes(-10), block.RangeBeginAsDateTime());
            Assert.Equal(_baseDateTime.AddMinutes(-40), block.RangeEndAsDateTime());
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public void If_OrderByMaxToDate_ThenReturnBlockWithMaxToDate()
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = sut.GetLastRangeBlock(CreateRequest(LastBlockOrder.MaxRangeEndValue));

            // ASSERT
            Assert.Equal(_block1, block.RangeBlockId);
            Assert.Equal(_baseDateTime.AddMinutes(-20), block.RangeBeginAsDateTime());
            Assert.Equal(_baseDateTime.AddMinutes(-30), block.RangeEndAsDateTime());
        }

        private LastBlockRequest CreateRequest(LastBlockOrder lastBlockOrder)
        {
            var request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), BlockType.DateRange);
            request.LastBlockOrder = lastBlockOrder;

            return request;
        }
    }
}
