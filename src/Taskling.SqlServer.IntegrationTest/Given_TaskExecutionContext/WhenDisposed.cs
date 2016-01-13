using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.IntegrationTest.Given_TaskExecutionContext
{
    [TestClass]
    public class WhenDisposed
    {
        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0)
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveDeathThreshold = new TimeSpan(0, 0, 2, 0)
            };

            // ACT
            var executionsHelper = new ExecutionsHelper();
            bool startedOk;
            byte tokenStatusAfterStart;
            byte tokenStatusAfterUsingBlock;

            var client = new SqlServerTasklingClient(settings);
            using (var executionContext = client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions))
            {
                startedOk = executionContext.TryStart();
                tokenStatusAfterStart = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
            }
            tokenStatusAfterUsingBlock = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

            // ASSERT
            Assert.AreEqual(true, startedOk);
            Assert.AreEqual((byte) TaskExecutionStatus.Unavailable, tokenStatusAfterStart);
            Assert.AreEqual((byte) TaskExecutionStatus.Available, tokenStatusAfterUsingBlock);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_KeepAlive_ThenKeepAliveContinuesUntilExecutionContextDies()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);
            
            // ACT
            StartContextWithoutUsingOrComplete();
            GC.Collect(0, GCCollectionMode.Forced); // referenceless context is collected
            Thread.Sleep(6000);

            // ASSERT
            var expectedLastKeepAliveMax = DateTime.UtcNow.AddSeconds(-5);
            var lastKeepAlive = executionHelper.GetLastKeepAlive(taskDefinitionId);
            Assert.IsTrue(lastKeepAlive < expectedLastKeepAliveMax);
        }

        private void StartContextWithoutUsingOrComplete()
        {
            var executionContext = SqlServerClientHelper.GetExecutionContextWithKeepAlive(new TimeSpan(0, 0, 1));
            executionContext.TryStart();
        }
    }
}
