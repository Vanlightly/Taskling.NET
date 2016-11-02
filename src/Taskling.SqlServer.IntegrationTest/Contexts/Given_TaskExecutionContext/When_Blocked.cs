using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_TaskExecutionContext
{
    [TestClass]
    public class When_Blocked
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
        public void If_TryStartOverTheConcurrencyLimit_ThenMarkExecutionAsBlocked()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();

            // ACT
            bool startedOk;
            bool startedOkBlockedExec;
            bool isBlocked;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                using (var executionContextBlocked = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    startedOkBlockedExec = executionContextBlocked.TryStart();
                }
                isBlocked = executionHelper.GetBlockedStatusOfLastExecution(_taskDefinitionId);
            }

            // ASSERT
            Assert.IsTrue(isBlocked);
            Assert.IsTrue(startedOk);
            Assert.IsFalse(startedOkBlockedExec);

        }
    }
}
