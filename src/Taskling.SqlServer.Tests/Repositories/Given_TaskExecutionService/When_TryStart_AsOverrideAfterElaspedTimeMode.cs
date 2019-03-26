using Xunit;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.Tests.Helpers;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;

namespace Taskling.SqlServer.Tests.Repositories.Given_TaskExecutionService
{
    public class When_TryStart_AsOverrideAfterElaspedTimeMode
    {
        public When_TryStart_AsOverrideAfterElaspedTimeMode()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
        }
        
        private TaskExecutionRepository CreateSut()
        {
            return new TaskExecutionRepository(new TaskRepository(), new ExecutionTokenRepository(new CommonTokenRepository()), new EventsRepository());
        }

        private TaskExecutionStartRequest CreateOverrideStartRequest(int concurrencyLimit = 1)
        {
            return new TaskExecutionStartRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), TaskDeathMode.Override, concurrencyLimit, 3, 3)
            {
                OverrideThreshold = new TimeSpan(0, 1, 0),
                TasklingVersion = "N/A"
            };
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_ThenReturnsValidDataValues()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateOverrideStartRequest();

            // ACT
            var sut = CreateSut();
            var response = sut.Start(startRequest);

            // ASSERT
            Assert.True(response.TaskExecutionId != "0");
            Assert.True(response.StartedAt > DateTime.MinValue);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateOverrideStartRequest();

            // ACT
            var sut = CreateSut();
            var response = sut.Start(startRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, response.GrantStatus);
            Assert.NotEqual("0", response.ExecutionTokenId);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateOverrideStartRequest();
            var secondStartRequest = CreateOverrideStartRequest();

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(firstStartRequest);
            var secondResponse = sut.Start(secondStartRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.Equal(GrantStatus.Denied, secondResponse.GrantStatus);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateOverrideStartRequest();
            var secondStartRequest = CreateOverrideStartRequest();

            // ACT
            var sut = CreateSut();
            var firstStartResponse = sut.Start(firstStartRequest);
            var firstCompleteRequest = new TaskExecutionCompleteRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
            var firstCompleteResponse = sut.Complete(firstCompleteRequest);

            var secondStartResponse = sut.Start(secondStartRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstStartResponse.GrantStatus);
            Assert.Equal(GrantStatus.Granted, secondStartResponse.GrantStatus);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
        {
            // ARRANGE
            int concurrencyLimit = 4;
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId, concurrencyLimit);

            var firstStartRequest = CreateOverrideStartRequest(concurrencyLimit);
            var secondStartRequest = CreateOverrideStartRequest(concurrencyLimit);
            var thirdStartRequest = CreateOverrideStartRequest(concurrencyLimit);
            var fourthStartRequest = CreateOverrideStartRequest(concurrencyLimit);
            var fifthStartRequest = CreateOverrideStartRequest(concurrencyLimit);

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(firstStartRequest);
            var secondResponse = sut.Start(secondStartRequest);
            var thirdResponse = sut.Start(thirdStartRequest);
            var fourthResponse = sut.Start(fourthStartRequest);
            var fifthResponse = sut.Start(fifthStartRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.Equal(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.Equal(GrantStatus.Granted, thirdResponse.GrantStatus);
            Assert.Equal(GrantStatus.Granted, fourthResponse.GrantStatus);
            Assert.Equal(GrantStatus.Denied, fifthResponse.GrantStatus);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_OneToken_MultipleTaskThreads_ThenNoDeadLocksOccur()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT
            var sut = CreateSut();
            var tasks = new List<Task>();
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithTimeOverrideMode, sut, TaskCreationOptions.LongRunning));

            Task.WaitAll(tasks.ToArray());

            // ASSERT

        }

        private void RequestAndReturnTokenWithTimeOverrideMode(object sutObj)
        {
            var sut = (TaskExecutionRepository)sutObj;
            for (int i = 0; i < 100; i++)
            {
                var firstStartRequest = CreateOverrideStartRequest();

                var firstStartResponse = sut.Start(firstStartRequest);

                if (firstStartResponse.GrantStatus == GrantStatus.Granted)
                {
                    var firstCompleteRequest = new TaskExecutionCompleteRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
                    var firstCompleteResponse = sut.Complete(firstCompleteRequest);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_OneTaskAndOneTokenAndIsUnavailableAndGrantedDateHasPassedElapsedTime_ThenIsGranted()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateOverrideStartRequest();
            startRequest.OverrideThreshold = new TimeSpan(0, 0, 5);
            var secondRequest = CreateOverrideStartRequest();
            secondRequest.OverrideThreshold = new TimeSpan(0, 0, 5);

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(startRequest);

            Thread.Sleep(6000);

            var secondResponse = sut.Start(secondRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.Equal(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.NotEqual("0", secondResponse.ExecutionTokenId);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public void If_TimeOverrideMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateOverrideStartRequest();
            var secondRequest = CreateOverrideStartRequest();

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(startRequest);

            Thread.Sleep(5000);

            var secondResponse = sut.Start(secondRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.NotEqual("0", firstResponse.ExecutionTokenId);
            Assert.Equal(GrantStatus.Denied, secondResponse.GrantStatus);
            Assert.Equal("0", secondResponse.ExecutionTokenId);
        }
    }
}
