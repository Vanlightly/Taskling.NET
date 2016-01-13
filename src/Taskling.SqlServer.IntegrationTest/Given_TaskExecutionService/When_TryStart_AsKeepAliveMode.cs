using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.TaskExecution;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.TaskExecution;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Given_TaskExecutionService
{
    [TestClass]
    public class When_TryStart_AsKeepAliveMode
    {
        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            
        }

        private TaskExecutionService CreateSut()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 1, 1)
            };
            return new TaskExecutionService(settings, new TaskService(settings));
        }

        private TaskExecutionStartRequest CreateKeepAliveStartRequest()
        {
            return new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive)
            {
                KeepAliveDeathThreshold = new TimeSpan(0, 1, 0),
                KeepAliveInterval = new TimeSpan(0, 0, 20)
            };
        }

        private SendKeepAliveRequest CreateKeepAliveRequest(string applicationName, string taskName, string taskExecutionId, string executionTokenId)
        {
            return new SendKeepAliveRequest()
            {
                ApplicationName = applicationName,
                TaskName = taskName,
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
            var firstCompleteRequest = new TaskExecutionCompleteRequest(TestConstants.ApplicationName, TestConstants.TaskName, firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
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
            var executionHelper = new ExecutionsHelper();
            var taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);
            executionHelper.InsertAvailableExecutionToken(taskDefinitionId);

            var firstStartRequest = CreateKeepAliveStartRequest();
            var secondStartRequest = CreateKeepAliveStartRequest();
            var thirdStartRequest = CreateKeepAliveStartRequest();
            var fourthStartRequest = CreateKeepAliveStartRequest();
            var fifthStartRequest = CreateKeepAliveStartRequest();

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(firstStartRequest);
            executionHelper.SetKeepAlive(taskDefinitionId, firstResponse.TaskExecutionId, firstResponse.ExecutionTokenId);
            var secondResponse = sut.Start(secondStartRequest);
            executionHelper.SetKeepAlive(taskDefinitionId, secondResponse.TaskExecutionId, secondResponse.ExecutionTokenId);
            var thirdResponse = sut.Start(thirdStartRequest);
            executionHelper.SetKeepAlive(taskDefinitionId, thirdResponse.TaskExecutionId, thirdResponse.ExecutionTokenId);
            var fourthResponse = sut.Start(fourthStartRequest);
            executionHelper.SetKeepAlive(taskDefinitionId, fourthResponse.TaskExecutionId, fourthResponse.ExecutionTokenId);
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
            var tuple = new Tuple<int, TaskExecutionService>(taskDefinitionId, sut);

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
            var tuple = (Tuple<int, TaskExecutionService>) state;
            var sut = tuple.Item2;
            for (int i = 0; i < 100; i++)
            {
                var firstStartRequest = CreateKeepAliveStartRequest();

                var firstStartResponse = sut.Start(firstStartRequest);

                var executionHelper = new ExecutionsHelper();
                executionHelper.SetKeepAlive(tuple.Item1, firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);

                if (firstStartResponse.GrantStatus == GrantStatus.Granted)
                {
                    var firstCompleteRequest = new TaskExecutionCompleteRequest(TestConstants.ApplicationName, TestConstants.TaskName, firstStartResponse.TaskExecutionId, firstStartResponse.ExecutionTokenId);
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
            executionHelper.SetKeepAlive(taskDefinitionId, firstResponse.TaskExecutionId, firstResponse.ExecutionTokenId);

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
            executionHelper.SetKeepAlive(taskDefinitionId, firstResponse.TaskExecutionId, firstResponse.ExecutionTokenId);

            Thread.Sleep(5000);

            var secondResponse = sut.Start(secondRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreNotEqual("0", firstResponse.ExecutionTokenId);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
            Assert.AreEqual("0", secondResponse.ExecutionTokenId);
        }
    }
}

