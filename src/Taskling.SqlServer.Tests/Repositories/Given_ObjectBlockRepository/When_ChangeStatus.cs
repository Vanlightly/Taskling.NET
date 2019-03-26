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

namespace Taskling.SqlServer.Tests.Repositories.Given_ObjectBlockRepository
{
    public class When_ChangeStatus
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        private int _taskDefinitionId;
        private string _taskExecution1;
        private DateTime _baseDateTime;
        private long _blockExecutionId;

        public When_ChangeStatus()
        {
            _blocksHelper = new BlocksHelper();
            _blocksHelper.DeleteBlocks(TestConstants.ApplicationName);
            _executionHelper = new ExecutionsHelper();
            _executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = _executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            _executionHelper.InsertUnlimitedExecutionToken(_taskDefinitionId);

            TaskRepository.ClearCache();
        }

        private ObjectBlockRepository CreateSut()
        {
            return new ObjectBlockRepository(new TaskRepository());
        }

        private void InsertObjectBlock()
        {
            _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

            _baseDateTime = new DateTime(2016, 1, 1);
            var block1 = _blocksHelper.InsertObjectBlock(_taskDefinitionId, DateTime.UtcNow, Guid.NewGuid().ToString()).ToString();
            _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(block1), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Started);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "Blocks")]
        public void If_SetStatusOfObjectBlock_ThenItemsCountIsCorrect()
        {
            // ARRANGE
            InsertObjectBlock();

            var request = new BlockExecutionChangeStatusRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                _taskExecution1,
                BlockType.Object,
                _blockExecutionId.ToString(),
                BlockExecutionStatus.Completed);
            request.ItemsProcessed = 10000;


            // ACT
            var sut = CreateSut();
            sut.ChangeStatus(request);

            var itemCount = new BlocksHelper().GetBlockExecutionItemCount(_blockExecutionId);

            // ASSERT
            Assert.Equal(10000, itemCount);
        }
    }
}
