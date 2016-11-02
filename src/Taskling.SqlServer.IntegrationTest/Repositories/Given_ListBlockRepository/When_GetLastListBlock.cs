using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.Blocks.Common;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.Blocks;
using Taskling.Serialization;
using Taskling.SqlServer.Blocks;
using Taskling.SqlServer.IntegrationTest.Helpers;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Repositories.Given_ListBlockRepository
{
    [TestClass]
    public class When_GetLastListBlock
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

        private ListBlockRepository CreateSut()
        {
            return new ListBlockRepository(new TaskRepository());
        }

        private void InsertBlocks()
        {
            _taskExecution1 = _executionHelper.InsertOverrideTaskExecution(_taskDefinitionId);

            _baseDateTime = new DateTime(2016, 1, 1);
            var dateRange1 = new DateRange() { FromDate = _baseDateTime.AddMinutes(-20), ToDate = _baseDateTime };
            _block1 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, JsonGenericSerializer.Serialize<DateRange>(dateRange1)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block1), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-20), _baseDateTime.AddMinutes(-25), BlockExecutionStatus.Failed);

            Thread.Sleep(10);
            var dateRange2 = new DateRange() { FromDate = _baseDateTime.AddMinutes(-30), ToDate = _baseDateTime };
            _block2 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, JsonGenericSerializer.Serialize<DateRange>(dateRange2)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block2), _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-30), _baseDateTime.AddMinutes(-35), BlockExecutionStatus.Started);

            Thread.Sleep(10);
            var dateRange3 = new DateRange() { FromDate = _baseDateTime.AddMinutes(-40), ToDate = _baseDateTime };
            _block3 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, JsonGenericSerializer.Serialize<DateRange>(dateRange3)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block3), _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-40), _baseDateTime.AddMinutes(-45), BlockExecutionStatus.NotStarted);

            Thread.Sleep(10);
            var dateRange4 = new DateRange() { FromDate = _baseDateTime.AddMinutes(-50), ToDate = _baseDateTime };
            _block4 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, JsonGenericSerializer.Serialize<DateRange>(dateRange4)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block4), _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-50), _baseDateTime.AddMinutes(-55), BlockExecutionStatus.Completed);

            Thread.Sleep(10);
            var dateRange5 = new DateRange() { FromDate = _baseDateTime.AddMinutes(-60), ToDate = _baseDateTime };
            _block5 = _blocksHelper.InsertListBlock(_taskDefinitionId, DateTime.UtcNow, JsonGenericSerializer.Serialize<DateRange>(dateRange5)).ToString();
            _blocksHelper.InsertBlockExecution(_taskExecution1, long.Parse(_block5), _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-60), _baseDateTime.AddMinutes(-65), BlockExecutionStatus.Started);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("Blocks")]
        public void ThenReturnLastCreated()
        {
            // ARRANGE
            InsertBlocks();

            // ACT
            var sut = CreateSut();
            var block = sut.GetLastListBlock(CreateRequest());

            // ASSERT
            Assert.AreEqual(_block5, block.ListBlockId);
            Assert.AreEqual(new DateTime(2016, 1, 1).AddMinutes(-60), JsonGenericSerializer.Deserialize<DateRange>(block.Header).FromDate);
            Assert.AreEqual(new DateTime(2016, 1, 1), JsonGenericSerializer.Deserialize<DateRange>(block.Header).ToDate);
        }


        private LastBlockRequest CreateRequest()
        {
            var request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), BlockType.Object);
            request.LastBlockOrder = LastBlockOrder.LastCreated;

            return request;
        }
    }
}
