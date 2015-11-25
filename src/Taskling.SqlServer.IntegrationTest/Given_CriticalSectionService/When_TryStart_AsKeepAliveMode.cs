using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.CriticalSections;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Given_CriticalSectionService
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

        private CriticalSectionService CreateSut()
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                TableSchema = "Taskling",
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 1, 1)
            };
            return new CriticalSectionService(settings, new TaskService(settings));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_KeepAliveMode_TokenAvailableAndNothingInQueue_ThenGrant()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            var taskExecutionId = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId);
            
            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName, 
                TestConstants.TaskName, 
                taskExecutionId, 
                TaskDeathMode.KeepAlive);
            request.KeepAliveElapsedSeconds = 60;
            
            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_KeepAliveMode_TokenNotAvailableAndNothingInQueue_ThenAddToQueueAndDeny()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

            // Create execution 1 and assign critical section to it
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertUnavailableCriticalSectionToken(taskSecondaryId, taskExecutionId1);
            
            // Create second execution
            var taskExecutionId2 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId2);

            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId2,
                TaskDeathMode.KeepAlive);
            request.KeepAliveElapsedSeconds = 60;

            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            var isInQueue = executionHelper.GetQueueCount(taskExecutionId2) == 1;
            Assert.AreEqual(true, isInQueue);
            Assert.AreEqual(GrantStatus.Denied, response.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_KeepAliveMode_TokenNotAvailableAndAlreadyInQueue_ThenDoNotAddToQueueAndDeny()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

            // Create execution 1 and assign critical section to it
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertUnavailableCriticalSectionToken(taskSecondaryId, taskExecutionId1);
            
            // Create second execution and insert into queue
            var taskExecutionId2 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId2);
            executionHelper.InsertIntoCriticalSectionQueue(taskSecondaryId, taskExecutionId2);
            
            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId2,
                TaskDeathMode.KeepAlive);
            request.KeepAliveElapsedSeconds = 600;

            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            var numberOfQueueRecords = executionHelper.GetQueueCount(taskExecutionId2);
            Assert.AreEqual(1, numberOfQueueRecords);
            Assert.AreEqual(GrantStatus.Denied, response.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_KeepAliveMode_TokenAvailableAndIsFirstInQueue_ThenRemoveFromQueueAndGrant()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

            // Create execution 1 and create available critical section token
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertIntoCriticalSectionQueue(taskSecondaryId, taskExecutionId1);
            executionHelper.InsertAvailableCriticalSectionToken(taskSecondaryId, 0);

            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId1,
                TaskDeathMode.KeepAlive);
            request.KeepAliveElapsedSeconds = 60;

            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            var numberOfQueueRecords = executionHelper.GetQueueCount(taskExecutionId1);
            Assert.AreEqual(0, numberOfQueueRecords);
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueue_ThenDoNotChangeQueueAndDeny()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

            // Create execution 1 and add it to the queue
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertIntoCriticalSectionQueue(taskSecondaryId, taskExecutionId1);
            
            // Create execution 2 and add it to the queue
            var taskExecutionId2 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId2);
            executionHelper.InsertIntoCriticalSectionQueue(taskSecondaryId, taskExecutionId2);

            // Create an available critical section token
            executionHelper.InsertAvailableCriticalSectionToken(taskSecondaryId, 0);

            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId2,
                TaskDeathMode.KeepAlive);
            request.KeepAliveElapsedSeconds = 60;

            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            var numberOfQueueRecords = executionHelper.GetQueueCount(taskExecutionId2);
            Assert.AreEqual(1, numberOfQueueRecords);
            Assert.AreEqual(GrantStatus.Denied, response.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueueButFirstHasExpiredTimeout_ThenRemoveBothFromQueueAndGrant()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);

            // Create execution 1 and add it to the queue
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertIntoCriticalSectionQueue(taskSecondaryId, taskExecutionId1);
            
            Thread.Sleep(6000);

            // Create execution 2 and add it to the queue
            var taskExecutionId2 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId2);
            executionHelper.InsertIntoCriticalSectionQueue(taskSecondaryId, taskExecutionId2);

            // Create an available critical section token
            executionHelper.InsertAvailableCriticalSectionToken(taskSecondaryId, 0);

            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId2,
                TaskDeathMode.KeepAlive);
            request.KeepAliveElapsedSeconds = 5;

            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            var numberOfQueueRecordsForExecution1 = executionHelper.GetQueueCount(taskExecutionId1);
            var numberOfQueueRecordsForExecution2 = executionHelper.GetQueueCount(taskExecutionId2);
            Assert.AreEqual(0, numberOfQueueRecordsForExecution1);
            Assert.AreEqual(0, numberOfQueueRecordsForExecution2);
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
        }

        
    }
}
