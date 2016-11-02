using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Taskling.SqlServer.IntegrationTest.Helpers;

namespace Taskling.SqlServer.IntegrationTest.Contexts.Given_TaskExecutionContext
{
    [TestClass]
    public class When_Start
    {
        private int _taskDefinitionId;

        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfApplication(TestConstants.ApplicationName);

            _taskDefinitionId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_TryStart_ThenLogCorrectTasklingVersion()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();

            // ACT
            bool startedOk;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart();
                var sqlServerImplAssembly =
                    AppDomain.CurrentDomain.GetAssemblies()
                        .First(x => x.FullName.Contains("Taskling.SqlServer"));
                FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(sqlServerImplAssembly.Location);
                string versionOfTaskling = fileVersionInfo.ProductVersion;
                var executionVersion = executionHelper.GetLastExecutionVersion(_taskDefinitionId);
                Assert.AreEqual(versionOfTaskling.Trim(), executionVersion.Trim());
            }

            // ASSERT
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_TryStartWithHeader_ThenGetHeaderReturnsTheHeader()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var myHeader = new MyHeader()
            {
                Name = "Jack",
                Id = 367
            };

            // ACT
            bool startedOk;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart<MyHeader>(myHeader);

                var myHeaderBack = executionContext.GetHeader<MyHeader>();
                Assert.AreEqual(myHeader.Name, myHeaderBack.Name);
                Assert.AreEqual(myHeader.Id, myHeaderBack.Id);
            }

            // ASSERT
        }

        [TestMethod]
        [TestCategory("FastIntegrationTest"), TestCategory("TaskExecutions")]
        public void If_TryStartWithHeader_ThenHeaderWrittenToDatabase()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var myHeader = new MyHeader()
            {
                Name = "Jack",
                Id = 367
            };

            // ACT
            bool startedOk;

            using (var executionContext = ClientHelper.GetExecutionContext(TestConstants.TaskName, ClientHelper.GetDefaultTaskConfigurationWithKeepAliveAndReprocessing()))
            {
                startedOk = executionContext.TryStart<MyHeader>(myHeader);

                var myHeaderBack = executionContext.GetHeader<MyHeader>();
            }

            var dbHelper = new ExecutionsHelper();
            var executionHeader = dbHelper.GetLastExecutionHeader(_taskDefinitionId);
            var expectedHeader = "{\"Name\":\"Jack\",\"Id\":367}";

            // ASSERT
            Assert.AreEqual(expectedHeader, executionHeader);
        }
    }
}
