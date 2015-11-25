using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Taskling.ExecutionContext;
using Taskling.SqlServer.Configuration;
using Taskling.SqlServer.IntegrationTest.TestHelpers;

namespace Taskling.SqlServer.IntegrationTest.Given_CriticalSectionContext
{
    [TestClass]
    public class When_ConcurrentNoDeadlocks
    {
        [TestInitialize]
        public void Initialize()
        {
            var executionHelper = new ExecutionsHelper();
            executionHelper.DeleteRecordsOfTask(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest")]
        public void If_MultipleConcurrentRequests_UsingManualRetry_ThenNoDeadlocks()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertUnlimitedExecutionToken(taskSecondaryId);
            
            // ACT
            var filename = string.Empty;
            var tasks = new List<Task>();
            for(int i = 0; i<100; i++)
                tasks.Add(Task.Factory.StartNew(RunJobWithParameterLessTryStart, filename, TaskCreationOptions.LongRunning));
            
            Task.WaitAll(tasks.ToArray());
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest")]
        public void If_MultipleConcurrentRequests_UsingManualRetry_ThenNoDeadlocks_WithLoggingToFile()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertUnlimitedExecutionToken(taskSecondaryId);

            // ACT
            var filename = "CSLOG_" + DateTime.Now.ToString("HH-mm-ss") + ".csv";
            File.WriteAllText(filename, "Time,Id,Event,Text" + Environment.NewLine);
            var tasks = new List<Task>();
            for (int i = 0; i < 100; i++)
                tasks.Add(Task.Factory.StartNew(RunJobWithParameterLessTryStart, filename, TaskCreationOptions.LongRunning));

            Task.WaitAll(tasks.ToArray());
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest")]
        public void If_MultipleConcurrentRequests_UsingBuiltInRetry_ThenNoDeadlocks()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertUnlimitedExecutionToken(taskSecondaryId);

            // ACT
            var filename = string.Empty;
            var tasks = new List<Task>();
            for (int i = 0; i < 100; i++)
                tasks.Add(Task.Factory.StartNew(RunJobWithTryStartWithBuiltInRetry, filename, TaskCreationOptions.LongRunning));

            Task.WaitAll(tasks.ToArray());
        }

        [TestMethod]
        [TestCategory("SlowIntegrationTest")]
        public void If_MultipleConcurrentRequests_UsingBuiltInRetry_ThenNoDeadlocks_WithLoggingToFile()
        {
            // ARRANGE
            var executionHelper = new ExecutionsHelper();
            var taskSecondaryId = executionHelper.InsertTask(TestConstants.ApplicationName, TestConstants.TaskName);
            executionHelper.InsertUnlimitedExecutionToken(taskSecondaryId);

            // ACT
            var filename = "CSLOG_" + DateTime.Now.ToString("HH-mm-ss") + ".csv";
            File.WriteAllText(filename, "Time,Id,Event,Text" + Environment.NewLine);
            var tasks = new List<Task>();
            for (int i = 0; i < 100; i++)
                tasks.Add(Task.Factory.StartNew(RunJobWithTryStartWithBuiltInRetry, filename, TaskCreationOptions.LongRunning));

            Task.WaitAll(tasks.ToArray());
        }

        private void RunJobWithParameterLessTryStart(object fileNameObj)
        {
            string fileName = fileNameObj.ToString();
            int counter = 0;
            int id = Guid.NewGuid().GetHashCode();
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };

            var client = new SqlServerTasklingClient(settings);
            using (var executionContext = client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions))
            {
                var startedOk = executionContext.TryStart();
                Append(fileName, id, "TE_TryStart", startedOk.ToString());
                if (startedOk)
                {
                    using (var criticalSection = executionContext.CreateCriticalSection())
                    {
                        while (!criticalSection.TryStart())
                        {
                            Append(fileName, id, "CS_TryStart", "Denied");
                            counter++;
                            Thread.Sleep(1000);
                        }

                        Append(fileName, id, "CS_TryStart", "Granted");
                    }

                    Append(fileName, id, "CS_TryComplete", "True");
                }
            }
        }

        private void RunJobWithTryStartWithBuiltInRetry(object fileNameObj)
        {
            string fileName = fileNameObj.ToString();
            int id = Guid.NewGuid().GetHashCode();
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = new TimeSpan(0, 0, 0, 30),
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };

            var client = new SqlServerTasklingClient(settings);
            using (var executionContext = client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions))
            {
                var startedOk = executionContext.TryStart();
                Append(fileName, id, "TE_TryStart", startedOk.ToString());
                if (startedOk)
                {
                    using (var criticalSection = executionContext.CreateCriticalSection())
                    {
                        Append(fileName, id, "CS_TryStartWithRetry", "Requested");
                        var csStartedOk = criticalSection.TryStart(new TimeSpan(0, 0, 1), 60);
                        Append(fileName, id, "CS_TryStartWithRetry", csStartedOk ? "Granted" : "Denied");
                    }
                    Append(fileName, id, "CS_TryComplete", "");
                }
            }
        }

        private object _obj = new object();
        private void Append(string fileName, int id, string eventType, string text)
        {
            if(!string.IsNullOrEmpty(fileName))
            {
                lock (_obj)
                {
                    File.AppendAllText(fileName,
                        DateTime.Now.ToString("HH:mm:ss:fff") + ", " + id + ", " + eventType + ", " + text +
                        Environment.NewLine);
                }
            }
        }
    }
}
