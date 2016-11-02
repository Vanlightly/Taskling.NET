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
    public class When_TryStart_AsOverrideAfterElaspedTimeMode
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

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.IsTrue(response.TaskExecutionId != "0");
            Assert.IsTrue(response.StartedAt > DateTime.MinValue);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
            Assert.AreNotEqual("0", response.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.AreEqual(GrantStatus.Granted, firstStartResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, secondStartResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, thirdResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, fourthResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Denied, fifthResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest"), TestCategory("ExecutionTokens")]
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

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.AreNotEqual("0", secondResponse.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
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
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreNotEqual("0", firstResponse.ExecutionTokenId);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
            Assert.AreEqual("0", secondResponse.ExecutionTokenId);
        }
    }
}
