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
            return client.CreateTaskExecutionContext(TestConstants.ApplicationName, TestConstants.TaskName, taskExecutionOptions);
        }
    }
}
