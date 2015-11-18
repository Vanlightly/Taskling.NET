using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.InfrastructureContracts.CriticalSections;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.CriticalSections;
using Taskling.SqlServer.IntegrationTest.TestHelpers;
using Taskling.SqlServer.Tasks;

namespace Taskling.SqlServer.IntegrationTest.Given_CriticalSectionService
{
    [TestClass]
    public class When_TryStart_AsOverrideAfterElaspedTimeMode
    {
        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
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
                TableSchema = "PC",
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 1, 1)
            };
            return new CriticalSectionService(settings, new TaskService(settings));
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_OverrideMode_TokenAvailable_ThenGrant()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            var taskExecutionId = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId);

            var request = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId,
                TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate);
            request.SecondsOverride = 60;
            
            // ACT
            var sut = CreateSut();
            var response = sut.Start(request);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, response.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_OverrideMode_TwoConcurrentExecutionsAndTokenAvailable_ThenGrantFirstAndDenySecond()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            var taskExecutionId2 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId2);

            var request1 = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId1,
                TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate);
            request1.SecondsOverride = 60;

            var request2 = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId2,
                TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate);
            request2.SecondsOverride = 60;

            // ACT
            var sut = CreateSut();
            var response1 = sut.Start(request1);
            var response2 = sut.Start(request2);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, response1.GrantStatus);
            Assert.AreEqual(GrantStatus.Denied, response2.GrantStatus);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("CriticalSectionTokens")]
        public void If_OverrideMode_TwoConcurrentExecutionsAndTokenAvailableAndFirstHasPassedOverrideLimit_ThenGrantFirstAndSecond()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper(TestConstants.TestConnectionString);
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            var taskExecutionId1 = executionHelper.InsertTaskExecution(taskSecondaryId);
            var taskExecutionId2 = executionHelper.InsertTaskExecution(taskSecondaryId);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId1);
            executionHelper.InsertExecutionToken(taskSecondaryId, 0, taskExecutionId2);

            var request1 = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId1,
                TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate);
            request1.SecondsOverride = 5;

            var request2 = new StartCriticalSectionRequest(TestConstants.ApplicationName,
                TestConstants.TaskName,
                taskExecutionId2,
                TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate);
            request2.SecondsOverride = 5;

            // ACT
            var sut = CreateSut();
            var response1 = sut.Start(request1);
            Thread.Sleep(6000);
            var response2 = sut.Start(request2);

            // ASSERT
            Assert.AreEqual(GrantStatus.Granted, response1.GrantStatus);
            Assert.AreEqual(GrantStatus.Granted, response2.GrantStatus);
        }
    }
}
