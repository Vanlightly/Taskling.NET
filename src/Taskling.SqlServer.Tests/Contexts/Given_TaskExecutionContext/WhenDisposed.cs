using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens.Executions;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext
{
    public class WhenDisposed
    {
        public WhenDisposed()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_InUsingBlockAndNoExecutionTokenExists_ThenExecutionTokenCreatedAutomatically()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

            // ACT
            var executionsHelper = new ExecutionsHelper();
            bool startedOk;
            ExecutionTokenStatus tokenStatusAfterStart;
            ExecutionTokenStatus tokenStatusAfterUsingBlock;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                tokenStatusAfterStart = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
            }
            tokenStatusAfterUsingBlock = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

            // ASSERT
            Assert.True(startedOk);
            Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
            Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public void If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT
            var executionsHelper = new ExecutionsHelper();
            bool startedOk;
            ExecutionTokenStatus tokenStatusAfterStart;
            ExecutionTokenStatus tokenStatusAfterUsingBlock;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                tokenStatusAfterStart = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
            }
            tokenStatusAfterUsingBlock = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

            // ASSERT
            Assert.True(startedOk);
            Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
            Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
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
            Assert.True(lastKeepAlive < expectedLastKeepAliveMax);
        }

        private void StartContextWithoutUsingOrComplete()
        {
            var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
            executionContext.TryStart();
        }
    }
}
