using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;

namespace Taskling.SqlServer.IntegrationTest.Given_TaskExecutionContext
{
    [TestClass]
    public class WhenDisposed
    {
        private const int TokenAvailable = 1;
        private const int TokenUnavailable = 0;

        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
            executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [TestMethod]
        public void If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
            executionHelper.CreateTaskAndExecutionToken(TestConstants.ApplicationName, TestConstants.TaskName);

            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };
            
            // ACT
            var executionsHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
            bool startedOk;
            byte tokenStatusAfterStart;
            byte tokenStatusAfterUsingBlock;

            var client = new SqlServerTasklingClient(settings);
            using (var executionContext = client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions))
            {
                startedOk = executionContext.TryStart();
                tokenStatusAfterStart = executionsHelper.GetTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
            }
            tokenStatusAfterUsingBlock = executionsHelper.GetTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

            // ASSERT
            Assert.AreEqual(true, startedOk);
            Assert.AreEqual(TokenUnavailable, tokenStatusAfterStart);
            Assert.AreEqual(TokenAvailable, tokenStatusAfterUsingBlock);
        }
    }
}
