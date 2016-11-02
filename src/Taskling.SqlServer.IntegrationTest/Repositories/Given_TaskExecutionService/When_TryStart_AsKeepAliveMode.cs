using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Events;
using Taskling.SqlServer.IntegrationTest.Helpers;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;
using Taskling.SqlServer.Tokens;
using Taskling.SqlServer.Tokens.Executions;
using Taskling.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Repositories.Given_TaskExecutionService
{
    [TestClass]
    public class When_TryStart_AsKeepAliveMode
    {
        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            TaskRepository.ClearCache();
        }

        [TestCleanup]
        public void TestCleanup()
        {

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

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_ThenReturnsValidDataValues()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var response = sut.Start(startRequest);

            // ASSERT
            Assert.IsTrue(response.TaskExecutionId != "0");
            Assert.IsTrue(response.StartedAt > DateTime.MinValue);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var response = sut.Start(startRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
            Assert.AreNotEqual("0", response.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(firstStartRequest);
            sut.SendKeepAlive(CreateKeepAliveRequest(TestConstants.ApplicationName, TestConstants.TaskName, firstResponse.TaskExecutionId, firstResponse.ExecutionTokenId));
            var secondResponse = sut.Start(secondStartRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var firstStartResponse = sut.Start(firstStartRequest);
            var firstCompleteRequest = new TaskExecutionCompleteRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
            var firstCompleteResponse = sut.Complete(firstCompleteRequest);

            var secondStartResponse = sut.Start(secondStartRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstStartResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, secondStartResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
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
            var firstResponse = sut.Start(firstStartRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);
            var secondResponse = sut.Start(secondStartRequest);
            executionHelper.SetKeepAlive(secondResponse.TaskExecutionId);
            var thirdResponse = sut.Start(thirdStartRequest);
            executionHelper.SetKeepAlive(thirdResponse.TaskExecutionId);
            var fourthResponse = sut.Start(fourthStartRequest);
            executionHelper.SetKeepAlive(fourthResponse.TaskExecutionId);
            var fifthResponse = sut.Start(fifthStartRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, thirdResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, fourthResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Denied, fifthResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest"), TestCategory("ExecutionTokens")]
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
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, tuple, TaskCreationOptions.LongRunning));

            Task.WaitAll(tasks.ToArray());

            // ASSERT

        }

        private void RequestAndReturnTokenWithKeepAliveMode(object state)
        {
            var tuple = (Tuple<int, TaskExecutionRepository>)state;
            var sut = tuple.Item2;
            for (int i = 0; i < 100; i++)
            {
                var firstStartRequest = CreateKeepAliveStartRequest();

                var firstStartResponse = sut.Start(firstStartRequest);

                var executionHelper = new ExecutionsHelper();
                executionHelper.SetKeepAlive(firstStartResponse.TaskExecutionId);

                if (firstStartResponse.GrantStatus == GrantStatus.Granted)
                {
                    var firstCompleteRequest = new TaskExecutionCompleteRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
                    var firstCompleteResponse = sut.Complete(firstCompleteRequest);
                }
            }
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasPassedElapsedTime_ThenIsGranted()
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
            var firstResponse = sut.Start(startRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(6000);

            var secondResponse = sut.Start(secondRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.AreNotEqual("0", secondResponse.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
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
            var firstResponse = sut.Start(startRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(5000);

            var secondResponse = sut.Start(secondRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreNotEqual("0", firstResponse.ExecutionTokenId);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
            Assert.AreEqual("0", secondResponse.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsFour_ThenCreateThreeNewTokens()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest(4);

            // ACT
            var sut = CreateSut();
            sut.Start(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.AreEqual(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
            Assert.AreEqual(3, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Available));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsUnlimited_ThenRemoveAvailableTokenAndCreateOneNewUnlimitedToken()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var startRequest = CreateKeepAliveStartRequest(-1);

            // ACT
            var sut = CreateSut();
            sut.Start(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.AreEqual(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unlimited));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneAvailableTokenAndOneUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveAvailableToken_AndSoDenyStart()
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
            var result = sut.Start(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.AreEqual(GrantStatus.Denied, result.GrantStatus);
            Assert.AreEqual(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_TwoUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveOneUnavailableToken()
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
            sut.Start(startRequest);

            // ASSERT
            var tokensList = executionHelper.GetExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
            Assert.AreEqual(1, tokensList.Tokens.Count(x => x.Status == ExecutionTokenStatus.Unavailable));
        }
    }
}
