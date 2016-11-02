using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Events;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_TaskExecutionContext
{
    [TestClass]
    public class When_Checkpoint
    {
        private int _taskDefinitionId;

        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
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
            Assert.AreEqual(EventType.CheckPoint, lastEvent.Item1);
            Assert.AreEqual("Test checkpoint", lastEvent.Item2);
        }
    }
}
