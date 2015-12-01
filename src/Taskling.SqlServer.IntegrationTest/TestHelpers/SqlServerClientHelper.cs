using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.SqlServer.Configuration;

namespace Taskling.SqlServer.IntegrationTest.TestHelpers
{
    public class SqlServerClientHelper
    {
        public static ITaskExecutionContext GetExecutionContextWithKeepAlive()
        {
            return GetExecutionContextWithKeepAlive(new TimeSpan(0, 0, 0, 30));
        }

        public static ITaskExecutionContext GetExecutionContextWithKeepAlive(TimeSpan keepAliveInterval)
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = keepAliveInterval,
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };

            var client = new SqlServerTasklingClient(settings);
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions);
        }

        public static ITaskExecutionContext GetExecutionContextWithKeepAlive(string taskName, TimeSpan keepAliveInterval)
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.KeepAlive,
                KeepAliveInterval = keepAliveInterval,
                KeepAliveElapsed = new TimeSpan(0, 0, 2, 0)
            };

            var client = new SqlServerTasklingClient(settings);
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, taskName, taskExecutionOptions);
        }

        public static ITaskExecutionContext GetExecutionContextWithOverride(string taskName, TimeSpan overrideTimespan)
        {
            var settings = new SqlServerClientConnectionSettings()
            {
                ConnectionString = TestConstants.TestConnectionString,
                ConnectTimeout = new TimeSpan(0, 0, 1, 0),
                TableSchema = TestConstants.TestTableSchema
            };

            var taskExecutionOptions = new TaskExecutionOptions()
            {
                TaskDeathMode = TaskDeathMode.OverrideAfterElapsedTimePeriodFromGrantDate,
                SecondsOverride = (int)overrideTimespan.TotalSeconds
            };

            var client = new SqlServerTasklingClient(settings);
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, taskName, taskExecutionOptions);
        }
    }
}
