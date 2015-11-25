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
            executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            
        }

        private TaskExecutionService CreateSut()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                TableSchema = "Taskling",
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 1, 1)
            };
            return new TaskExecutionService(settings, new TaskService(settings));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_ThenReturnsValidDataValues()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var startRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            startRequest.KeepAliveElapsedSeconds = 60;

            // ACT
            var sut = CreateSut();
            var response = sut.Start(startRequest);

            // ASSERT
            Assert.IsTrue(response.TaskExecutionId > 0);
            Assert.IsTrue(response.StartedAt > DateTime.MinValue);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var startRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            startRequest.KeepAliveElapsedSeconds = 60;
            
            // ACT
            var sut = CreateSut();
            var response = sut.Start(startRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
            Assert.AreNotEqual(Guid.Empty, response.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var firstStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            firstStartRequest.KeepAliveElapsedSeconds = 60;

            var secondStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            secondStartRequest.KeepAliveElapsedSeconds = 60;

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(firstStartRequest);
            sut.SendKeepAlive(firstResponse.TaskExecutionId);
            var secondResponse = sut.Start(secondStartRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_TwoConcurrentTasksAndOneTokenAndIsUnlimited_ThenIsGrantBoth()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertUnlimitedExecutionToken(taskSecondaryId);

            var firstStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            firstStartRequest.KeepAliveElapsedSeconds = 60;

            var secondStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            secondStartRequest.KeepAliveElapsedSeconds = 60;

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(firstStartRequest);
            sut.SendKeepAlive(firstResponse.TaskExecutionId);
            var secondResponse = sut.Start(secondStartRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.GrantedWithoutLimit, firstResponse.GrantStatus);
            Assert.AreEqual(GrantStatus.GrantedWithoutLimit, secondResponse.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var firstStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            firstStartRequest.KeepAliveElapsedSeconds = 60;

            var secondStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            secondStartRequest.KeepAliveElapsedSeconds = 60;

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
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var firstStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            firstStartRequest.KeepAliveElapsedSeconds = 60;

            var secondStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            secondStartRequest.KeepAliveElapsedSeconds = 60;

            var thirdStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            thirdStartRequest.KeepAliveElapsedSeconds = 60;

            var fourthStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            fourthStartRequest.KeepAliveElapsedSeconds = 60;

            var fifthStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            fifthStartRequest.KeepAliveElapsedSeconds = 60;

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
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            // ACT
            var sut = CreateSut();
            var tasks = new List<Task>();
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));
            tasks.Add(Task.Factory.StartNew(RequestAndReturnTokenWithKeepAliveMode, sut, TaskCreationOptions.LongRunning));

            Task.WaitAll(tasks.ToArray());
            
            // ASSERT

        }

        private void RequestAndReturnTokenWithKeepAliveMode(object sutObj)
        {
            var sut = (TaskExecutionService) sutObj;
            for (int i = 0; i < 100; i++)
            {
                var firstStartRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
                firstStartRequest.KeepAliveElapsedSeconds = 60;

                var firstStartResponse = sut.Start(firstStartRequest);

                var executionHelper = new ExecutionsHelper();
                executionHelper.SetKeepAlive(firstStartResponse.TaskExecutionId);

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
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var startRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            startRequest.KeepAliveElapsedSeconds = 5;

            var secondRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            secondRequest.KeepAliveElapsedSeconds = 5;

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(startRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(6000);

            var secondResponse = sut.Start(secondRequest);
            
            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, secondResponse.GrantStatus);
            Assert.AreNotEqual(Guid.Empty, secondResponse.ExecutionTokenId);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("ExecutionTokens")]
        public void If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertAvailableExecutionToken(taskSecondaryId);

            var startRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            startRequest.KeepAliveElapsedSeconds = 6000;

            var secondRequest = new TaskExecutionStartRequest(TestConstants.ApplicationName, TestConstants.TaskName, TaskDeathMode.KeepAlive, null);
            secondRequest.KeepAliveElapsedSeconds = 6000;

            // ACT
            var sut = CreateSut();
            var firstResponse = sut.Start(startRequest);
            executionHelper.SetKeepAlive(firstResponse.TaskExecutionId);

            Thread.Sleep(5000);

            var secondResponse = sut.Start(secondRequest);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, firstResponse.GrantStatus);
            Assert.AreNotEqual(Guid.Empty, firstResponse.ExecutionTokenId);
            Assert.AreEqual(GrantStatus.Denied, secondResponse.GrantStatus);
            Assert.AreEqual(Guid.Empty, secondResponse.ExecutionTokenId);
        }
    }
}

