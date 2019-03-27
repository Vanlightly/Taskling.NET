using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.SqlServer.Tests.Helpers;
using System.Threading.Tasks;
using System.Threading;

namespace Taskling.SqlServer.Tests.Contexts.Given_TaskExecutionContext
{
    public class When_Blocked
    {
        private int _taskDefinitionId;

        public When_Blocked()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }
                
        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "TaskExecutions")]
        public async Task If_TryStartOverTheConcurrencyLimit_ThenMarkExecutionAsBlocked()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();

            // ACT
            bool startedOk;
            bool startedOkBlockedExec;
            bool isBlocked;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = await executionContext.TryStartAsync();
                using (var executionContextBlocked = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
                {
                    startedOkBlockedExec = await executionContextBlocked.TryStartAsync();
                }
                isBlocked = executionHelper.GetBlockedStatusOfLastExecution(_taskDefinitionId);
            }

            // ASSERT
            Assert.True(isBlocked);
            Assert.True(startedOk);
            Assert.False(startedOkBlockedExec);

        }
    }
}
