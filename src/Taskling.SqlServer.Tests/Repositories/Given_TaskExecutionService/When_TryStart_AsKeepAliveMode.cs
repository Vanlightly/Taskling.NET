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
    public class When_TryStart_AsKeepAliveMode
    {
        public When_TryStart_AsKeepAliveMode()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            TaskRepository.ClearCache();
        }
        
        private TaskExecutionRepository CreateSut()
        {
            return new TaskExecutionRepository(new TaskRepository(), new ExecutionTokenRepository(new CommonTokenRepository()), new EventsRepository());
        }

        private TaskExecutionStartRequest CreateKeepAliveStartRequest(int concurrencyLimit = 1)
        {
            return new TaskExecutionStartRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), TaskDeathMode.KeepAlive, concurrencyLimit, 3, 3)
            {
                KeepAliveDeathThreshold = new TimeSpan(0, 1, 0),
                KeepAliveInterval = new TimeSpan(0, 0, 20),
                TasklingVersion = "N/A"
            };
        }

        private SendKeepAliveRequest CreateKeepAliveRequest(string applicationName, string taskName, string taskExecutionId, string executionTokenId)
        {
            return new SendKeepAliveRequest()
            {
                TaskId = new TaskId(applicationName, taskName),
                TaskExecutionId = taskExecutionId,
                ExecutionTokenId = executionTokenId
            };
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_ThenReturnsValidDataValues()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var response = await sut.StartAsync(startRequest);

            // ASSERT
            Assert.True(response.TaskExecutionId != "0");
            Assert.True(response.StartedAt > DateTime.MinValue);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var response = await sut.StartAsync(startRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, response.GrantStatus);
            Assert.NotEqual("0", response.ExecutionTokenId);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var firstResponse = await sut.StartAsync(firstStartRequest);
            await sut.SendKeepAliveAsync(CreateKeepAliveRequest(TestConstants.ApplicationName, TestConstants.TaskName, firstResponse.TaskExecutionId, firstResponse.ExecutionTokenId));
            var secondResponse = await sut.StartAsync(secondStartRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.Equal(GrantStatus.Denied, secondResponse.GrantStatus);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var firstStartResponse = await sut.StartAsync(firstStartRequest);
            var firstCompleteRequest = new TaskExecutionCompleteRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
            var firstCompleteResponse = await sut.CompleteAsync(firstCompleteRequest);

            var secondStartResponse = await sut.StartAsync(secondStartRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstStartResponse.GrantStatus);
            Assert.Equal(GrantStatus.Granted, secondStartResponse.GrantStatus);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
        {
            // ARRANGE
            int concurrencyLimit = 4;
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId, concurrencyLimit);

            var firstStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var secondStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var thirdStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var fourthStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);
            var fifthStartRequest = CreateKeepAliveStartRequest(concurrencyLimit);

            // ACT
            var sut = CreateSut();
            var firstResponse = await sut.StartAsync(firstStartRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);
            var secondResponse = await sut.StartAsync(secondStartRequest);
            executionHelper.SetKeepAlive(secondResponse.TaskExecutionId);
            var thirdResponse = await sut.StartAsync(thirdStartRequest);
            executionHelper.SetKeepAlive(thirdResponse.TaskExecutionId);
            var fourthResponse = await sut.StartAsync(fourthStartRequest);
            executionHelper.SetKeepAlive(fourthResponse.TaskExecutionId);
            var fifthResponse = await sut.StartAsync(fifthStartRequest);

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
        public void If_KeepAliveMode_OneToken_MultipleTaskThreads_ThenNoDeadLocksOccur()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            // ACT
            var sut = CreateSut();
            var tuple = new Tuple<int, TaskExecutionRepository>(taskDefinitionId, sut);

            var tasks = new List<Task>();
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));
            tasks.Add(Task.Run(async () => await RequestAndReturnTokenWithKeepAliveModeAsync(tuple)));

            Task.WaitAll(tasks.ToArray());

            // ASSERT

        }

        private async Task RequestAndReturnTokenWithKeepAliveModeAsync(Tuple<int, TaskExecutionRepository> tuple)
        {
            var sut = tuple.Item2;
            for (int i = 0; i < 100; i++)
            {
                var firstStartRequest = CreateKeepAliveStartRequest();

                var firstStartResponse = await sut.StartAsync(firstStartRequest);

                var executionHelper = new ExecutionsHelper();
                executionHelper.SetKeepAlive(firstStartResponse.TaskExecutionId);

                if (firstStartResponse.GrantStatus == GrantStatus.Granted)
                {
                    var firstCompleteRequest = new TaskExecutionCompleteRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
                    var firstCompleteResponse = await sut.CompleteAsync(firstCompleteRequest);
                }
            }
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasPassedElapsedTime_ThenIsGranted()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();
            startRequest.KeepAliveDeathThreshold = new TimeSpan(0, 0, 4);

            var secondRequest = CreateKeepAliveStartRequest();
            secondRequest.KeepAliveDeathThreshold = new TimeSpan(0, 0, 4);

            // ACT
            var sut = CreateSut();
            var firstResponse = await sut.StartAsync(startRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(6000);

            var secondResponse = await sut.StartAsync(secondRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.NotEqual("0", secondResponse.ExecutionTokenId);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();
            startRequest.KeepAliveDeathThreshold = new TimeSpan(1, 0, 0);

            var secondRequest = CreateKeepAliveStartRequest();
            secondRequest.KeepAliveDeathThreshold = new TimeSpan(1, 0, 0);

            // ACT
            var sut = CreateSut();
            var firstResponse = await sut.StartAsync(startRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(5000);

            var secondResponse = await sut.StartAsync(secondRequest);

            // ASSERT
            Assert.Equal(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.NotEqual("0", firstResponse.ExecutionTokenId);
            Assert.Equal(GrantStatus.Denied, secondResponse.GrantStatus);
            Assert.Equal("0", secondResponse.ExecutionTokenId);
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsFour_ThenCreateThreeNewTokens()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest(4);

            // ACT
            var sut = CreateSut();
            await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.Equal(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
            Assert.Equal(3, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Available));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsUnlimited_ThenRemoveAvailableTokenAndCreateOneNewUnlimitedToken()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest(-1);

            // ACT
            var sut = CreateSut();
            await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.Equal(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unlimited));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_OneAvailableTokenAndOneUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveAvailableToken_AndSoDenyStart()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertExecutionToken(taskDefinitionId, new List<Tuple<ExecutionTokenStatus, string>>()
            {
                new Tuple<ExecutionTokenStatus, string>(ExecutionTokenStatus.Unavailable, "0"),
                new Tuple<ExecutionTokenStatus, string>(ExecutionTokenStatus.Available, "1")
            });

            var startRequest = CreateKeepAliveStartRequest(1);

            // ACT
            var sut = CreateSut();
            var result = await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.Equal(GrantStatus.Denied, result.GrantStatus);
            Assert.Equal(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
        }

        [Fact]
        [Trait("Speed", "Fast")]
        [Trait("Area", "ExecutionTokens")]
        public async Task If_KeepAliveMode_TwoUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveOneUnavailableToken()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertExecutionToken(taskDefinitionId, new List<Tuple<ExecutionTokenStatus, string>>()
            {
                new Tuple<ExecutionTokenStatus, string>(ExecutionTokenStatus.Unavailable, "0"),
                new Tuple<ExecutionTokenStatus, string>(ExecutionTokenStatus.Unavailable, "1")
            });

            var startRequest = CreateKeepAliveStartRequest(1);

            // ACT
            var sut = CreateSut();
            await sut.StartAsync(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.Equal(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
        }
    }
}
