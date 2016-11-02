using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks.CommonRequests;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.IntegrationTest.Helpers;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Repositories.Given_RangeBlockRepository
{
    [TestClass]
    public class When_ChangeStatus
    {
        private ExecutionsHelper _executionHelper;
        private BlocksHelper _blocksHelper;

        private int _taskDefinitionId;
        private string _taskExecution1;
        private DateTime _baseDateTime;
        private long _blockExecutionId;

        [TestInitialize]
        public void Initialize()
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

        private void InsertDateRangeBlock()
        {
            _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

            _baseDateTime = new DateTime(2016, 1, 1);
            var block1 = _blocksHelper.InsertDateRangeBlock(_taskDefinitionId, _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-30), DateTime.UtcNow).ToString();
            _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(block1), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Started);
        }

        private void InsertNumericRangeBlock()
        {
            _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

            _baseDateTime = new DateTime(2016, 1, 1);
            var block1 = _blocksHelper.InsertNumericRangeBlock(_taskDefinitionId, 1, 100, DateTime.UtcNow).ToString();
            _blockExecutionId = _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(block1), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Started);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_SetStatusOfDateRangeBlock_ThenItemsCountIsCorrect()
        {
            // ARRANGE
            InsertDateRangeBlock();

            var request = new BlockExecutionChangeStatusRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                _taskExecution1,
                BlockType.DateRange,
                _blockExecutionId.ToString(),
                BlockExecutionStatus.Completed);
            request.ItemsProcessed = 10000;


            // ACT
            var sut = CreateSut();
            sut.ChangeStatus(request);

            var itemCount = new BlocksHelper().GetBlockExecutionItemCount(_blockExecutionId);

            // ASSERT
            Assert.AreEqual(itemCount, 10000);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void If_SetStatusOfNumericRangeBlock_ThenItemsCountIsCorrect()
        {
            // ARRANGE
            InsertNumericRangeBlock();

            var request = new BlockExecutionChangeStatusRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                _taskExecution1,
                BlockType.NumericRange,
                _blockExecutionId.ToString(),
                BlockExecutionStatus.Completed);
            request.ItemsProcessed = 10000;


            // ACT
            var sut = CreateSut();
            sut.ChangeStatus(request);

            var itemCount = new BlocksHelper().GetBlockExecutionItemCount(_blockExecutionId);

            // ASSERT
            Assert.AreEqual(itemCount, 10000);
        }
    }
}
