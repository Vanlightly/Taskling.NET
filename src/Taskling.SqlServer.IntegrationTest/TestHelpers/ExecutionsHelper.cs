using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.ExecutionContext;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.IntegrationTest.TestHelpers
{
    public class ExecutionsHelper
    {
        #region .: Queries :.

        #region .: Execution Tokens :.

        private const string InsertExecutionTokenQuery = @"
        INSERT INTO [Taskling].[ExecutionToken]
           ([TaskDefinitionId]
           ,[DateGranted]
           ,[DateReturned]
           ,[Status]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskDefinitionId
           ,GETUTCDATE()
           ,NULL
           ,@Status
           ,@TaskExecutionId
           ,@TaskExecutionId);

SELECT CAST(SCOPE_IDENTITY() AS INT);";

        private const string GetExecutionTokenStatusByTaskExecutionQuery = @"SELECT ET.[Status]
FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[ExecutionToken] ET ON TE.TaskExecutionId = ET.TaskExecutionId
JOIN [Taskling].[TaskDefinition] T ON ET.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Execution Tokens :.

        #region .: Delete All :.

        private const string DeleteExecutionTokenQuery = @"
DELETE CSQ FROM [Taskling].[CriticalSectionQueue] CSQ
JOIN [Taskling].[TaskDefinition] T ON CSQ.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

DELETE CST FROM [Taskling].[CriticalSectionToken] CST
JOIN [Taskling].[TaskDefinition] T ON CST.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

DELETE ET FROM [Taskling].[ExecutionToken] ET
JOIN [Taskling].[TaskDefinition] T ON ET.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

DELETE TE FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName;

DELETE FROM [Taskling].[TaskDefinition] 
WHERE ApplicationName = @ApplicationName";

        #endregion .: Delete All :.

        #region .: Tasks :.

        private const string InsertTaskQuery = @"INSERT INTO [Taskling].[TaskDefinition]([ApplicationName],[TaskName])
VALUES(@ApplicationName,@TaskName);

SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskDefinitionId]
  FROM [Taskling].[TaskDefinition]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Tasks :.

        #region .: Keep Alive :.

        private const string SetKeepAliveQuery = @"
UPDATE ET
SET [LastKeepAlive] = GETUTCDATE()
FROM [Taskling].[ExecutionToken] ET
WHERE [TaskDefinitionId] = @TaskDefinitionId
AND [ExecutionTokenId] = @ExecutionTokenId;

UPDATE TE
SET [LastKeepAlive] = @KeepAliveDateTime
FROM [Taskling].[TaskExecution] TE
WHERE [TaskExecutionId] = @TaskExecutionId;
";

        private const string GetLastKeepAliveQuery = @"SELECT MAX(LastKeepAlive)
FROM [Taskling].[ExecutionToken] 
WHERE [TaskDefinitionId] = @TaskDefinitionId";

        #endregion .: Keep Alive :.
        
        #region .: Task Executions :.

        private const string InsertKeepAliveTaskExecutionQuery = @"INSERT INTO [Taskling].[TaskExecution]
           ([TaskDefinitionId]
           ,[StartedAt]
           ,[LastKeepAlive]
           ,[ServerName]
           ,[TaskDeathMode]
           ,[KeepAliveInterval]
           ,[KeepAliveDeathThreshold])
     VALUES
           (@TaskDefinitionId
           ,GETUTCDATE()
           ,GETUTCDATE()
           ,@ServerName
           ,@TaskDeathMode
           ,@KeepAliveInterval
           ,@KeepAliveDeathThreshold);

SELECT CAST(SCOPE_IDENTITY() AS INT);
";

        private const string InsertOverrideTaskExecutionQuery = @"INSERT INTO [Taskling].[TaskExecution]
           ([TaskDefinitionId]
           ,[StartedAt]
           ,[LastKeepAlive]
           ,[ServerName]
           ,[TaskDeathMode]
           ,[OverrideThreshold])
     VALUES
           (@TaskDefinitionId
           ,GETUTCDATE()
           ,GETUTCDATE()
           ,@ServerName
           ,@TaskDeathMode
           ,@OverrideThreshold);

SELECT CAST(SCOPE_IDENTITY() AS INT);
";

        private const string UpdateTaskExecutionStatusQuery = @"
UPDATE [TasklingDb].[Taskling].[TaskExecution]
SET [CompletedAt] = GETUTCDATE()
WHERE TaskExecutionId = @TaskExecutionId
";

        #endregion .: Task Executions :.

        #region .: Critical Sections :.

        private const string InsertCriticalSectionTokenQuery = @"INSERT INTO [Taskling].[CriticalSectionToken]
           ([TaskDefinitionId]
           ,[DateGranted]
           ,[DateReturned]
           ,[Status]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskDefinitionId
           ,GETUTCDATE()
           ,NULL
           ,@Status
           ,@TaskExecutionId
           ,@TaskExecutionId)";

        private const string GetQueueCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[CriticalSectionQueue]
WHERE [TaskExecutionId] = @TaskExecutionId";

        private const string InsertIntoCriticalSectionQueueQuery = @"INSERT INTO [Taskling].[CriticalSectionQueue]
           ([TaskDefinitionId]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskDefinitionId
           ,@TaskExecutionId
           ,@TaskExecutionId)";

        private const string GetCriticalSectionTokenStatusByTaskExecutionQuery = @"SELECT CST.[Status]
FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[CriticalSectionToken] CST ON TE.TaskExecutionId = CST.TaskExecutionId
JOIN [Taskling].[TaskDefinition] T ON CST.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Critical Sections :.

        #endregion .: Queries :.


        public void DeleteRecordsOfApplication(string applicationName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = DeleteExecutionTokenQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.ExecuteNonQuery();
            }
        }

        public void SetKeepAlive(int taskDefinitionId, string taskExecutionId, string executionTokenId)
        {
            SetKeepAlive(taskDefinitionId, taskExecutionId, executionTokenId, DateTime.UtcNow);
        }

        public void SetKeepAlive(int taskDefinitionId, string taskExecutionId, string executionTokenId, DateTime keepAliveDateTime)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = SetKeepAliveQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@ExecutionTokenId", SqlDbType.Int).Value = executionTokenId;
                command.Parameters.Add("@KeepAliveDateTime", SqlDbType.DateTime).Value = keepAliveDateTime;
                
                command.ExecuteNonQuery();
            }
        }

        public DateTime GetLastKeepAlive(int taskDefinitionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetLastKeepAliveQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                return (DateTime)command.ExecuteScalar();
            }
        }

        #region .: Tasks :.

        //public int CreateTaskAndExecutionToken(string applicationName, string taskName, int tokenCount = 1)
        //{
        //    var taskDefinitionId = InsertTask(applicationName, taskName);
        //    InsertUnavailableExecutionToken(taskDefinitionId, 0);

        //    return taskDefinitionId;
        //}

        public int InsertTask(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertTaskQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    return int.Parse(reader["TaskDefinitionId"].ToString());
                }
            }

            return -1;
        }

        #endregion .: Tasks :.


        #region .: Execution Tokens :.

        public string InsertUnavailableExecutionToken(int taskDefinitionId)
        {
            return InsertExecutionToken(taskDefinitionId, TaskExecutionStatus.Unavailable, "0");
        }

        public string InsertAvailableExecutionToken(int taskDefinitionId)
        {
            return InsertExecutionToken(taskDefinitionId, TaskExecutionStatus.Available, "0");
        }

        public string InsertExecutionToken(int taskDefinitionId, TaskExecutionStatus status, string taskExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertExecutionTokenQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@Status", SqlDbType.Int).Value = (int)status;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                var executionTokenId = (int) command.ExecuteScalar();
                return executionTokenId.ToString();
            }
        }

        public byte GetExecutionTokenStatus(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetExecutionTokenStatusByTaskExecutionQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                var result = command.ExecuteScalar();
                if (result == DBNull.Value)
                    return 0;

                return (byte) result;
            }
        }

        #endregion .: Execution Tokens :.


        #region .: Task Executions :.

        public string InsertKeepAliveTaskExecution(int taskDefinitionId)
        {
            return InsertKeepAliveTaskExecution(taskDefinitionId, new TimeSpan(0, 1, 0), new TimeSpan(0, 0, 20));
        }

        public string InsertOverrideTaskExecution(int taskDefinitionId)
        {
            return InsertOverrideTaskExecution(taskDefinitionId, new TimeSpan(0, 1, 0));
        }

        private string InsertKeepAliveTaskExecution(int taskDefinitionId, TimeSpan keepAliveInterval, TimeSpan keepAliveDeathThreshold)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertKeepAliveTaskExecutionQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value = Environment.MachineName;
                command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.TinyInt)).Value = (byte)TaskDeathMode.KeepAlive;
                command.Parameters.Add(new SqlParameter("@KeepAliveInterval", SqlDbType.Time)).Value = keepAliveInterval;
                command.Parameters.Add(new SqlParameter("@KeepAliveDeathThreshold", SqlDbType.Time)).Value = keepAliveDeathThreshold;

                return command.ExecuteScalar().ToString();
            }
        }

        private string InsertOverrideTaskExecution(int taskDefinitionId, TimeSpan overrideThreshold)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertOverrideTaskExecutionQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.VarChar, 200)).Value = Environment.MachineName;
                command.Parameters.Add(new SqlParameter("@TaskDeathMode", SqlDbType.TinyInt)).Value = (byte)TaskDeathMode.Override;
                command.Parameters.Add(new SqlParameter("@OverrideThreshold", SqlDbType.Time)).Value = overrideThreshold;

                return command.ExecuteScalar().ToString();
            }
        }

        public void SetTaskExecutionAsCompleted(string taskExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = UpdateTaskExecutionStatusQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;

                command.ExecuteNonQuery();
            }
        }
        
        #endregion .: Task Executions :.


        #region .: Critical Sections :.

        public void InsertUnavailableCriticalSectionToken(int taskDefinitionId, string taskExecutionId)
        {
            InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 0);
        }

        public void InsertAvailableCriticalSectionToken(int taskDefinitionId, string taskExecutionId)
        {
            InsertCriticalSectionToken(taskDefinitionId, taskExecutionId, 1);
        }

        private void InsertCriticalSectionToken(int taskDefinitionId, string taskExecutionId, byte status)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertCriticalSectionTokenQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = status;
                command.ExecuteNonQuery();
            }
        }

        public int GetQueueCount(string taskExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetQueueCountQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                return (int)command.ExecuteScalar();
            }
        }

        public void InsertIntoCriticalSectionQueue(int taskDefinitionId, string taskExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertIntoCriticalSectionQueueQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.ExecuteNonQuery();
            }
        }

        public byte GetCriticalSectionTokenStatus(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetCriticalSectionTokenStatusByTaskExecutionQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                return (byte)command.ExecuteScalar();
            }
        }

        #endregion .: Critical Sections :.
    }
}
