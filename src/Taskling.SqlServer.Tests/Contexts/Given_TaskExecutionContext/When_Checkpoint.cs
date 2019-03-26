using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Events;
using Taskling.SqlServer.Tests.Helpers;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext
{
    public class When_Checkpoint
    {
        private int _taskDefinitionId;

        public When_Checkpoint()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_Checkpoint_ThenCheckpointEventCreated()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();

            // ACT
            bool startedOk;
            Tuple<EventType, string> lastEvent = null;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                executionContext.Checkpoint("Test checkpoint");
                lastEvent = executionHelper.GetLastEvent(_taskDefinitionId);
            }

            // ASSERT
            Assert.Equal(EventType.CheckPoint, lastEvent.Item1);
            Assert.Equal("Test checkpoint", lastEvent.Item2);
        }
    }
}
