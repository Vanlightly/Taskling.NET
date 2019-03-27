using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.Tokens.Executions;
using System.Threading.Tasks;

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
        public async Task If_InUsingBlockAndNoExecutionTokenExists_ThenExecutionTokenCreatedAutomatically()
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
                startedOk = await executionContext.TryStartAsync();
                tokenStatusAfterStart = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
            }

            await Task.Delay(1000);
            tokenStatusAfterUsingBlock = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

            // ASSERT
            Assert.True(startedOk);
            Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
            Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
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
                startedOk = await executionContext.TryStartAsync();
                tokenStatusAfterStart = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
            }

            await Task.Delay(1000);

            tokenStatusAfterUsingBlock = executionsHelper.GetExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

            // ASSERT
            Assert.True(startedOk);
            Assert.Equal(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
            Assert.Equal(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_KeepAlive_ThenKeepAliveContinuesUntilExecutionContextDies()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT
            await StartContextWithoutUsingOrCompleteAsync();
            GC.Collect(0, GCCollectionMode.Forced); // referenceless context is collected
            Thread.Sleep(6000);

            // ASSERT
            var expectedLastKeepAliveMax = DateTime.UtcNow.AddSeconds(-5);
            var lastKeepAlive = executionHelper.GetLastKeepAlive(taskDefinitionId);
            Assert.True(lastKeepAlive < expectedLastKeepAliveMax);
        }

        private async Task StartContextWithoutUsingOrCompleteAsync()
        {
            var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing());
            await executionContext.TryStartAsync();
        }
    }
}
