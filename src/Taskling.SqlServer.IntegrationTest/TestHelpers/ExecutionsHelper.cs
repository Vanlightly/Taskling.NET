using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.SqlServer.TaskExecution;

namespace Taskling.SqlServer.IntegrationTest.TestHelpers
{
    public class ExecutionsHelper
    {
        #region .: Queries :.

        #region .: Execution Tokens :.

        private const string InsertExecutionTokenQuery = @"
        INSERT INTO [Taskling].[ExecutionToken]
           ([TaskSecondaryId]
           ,[DateGranted]
           ,[DateReturned]
           ,[Status]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId]
           ,[LastKeepAlive])
     VALUES
           (@TaskSecondaryId
           ,GETUTCDATE()
           ,NULL
           ,@Status
           ,@TaskExecutionId
           ,@TaskExecutionId
           ,GETUTCDATE());";

        private const string GetExecutionTokenStatusByTaskExecutionQuery = @"SELECT ET.[Status]
FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[ExecutionToken] ET ON TE.TaskExecutionId = ET.TaskExecutionId
JOIN [Taskling].[Task] T ON ET.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Execution Tokens :.

        #region .: Delete All :.

        private const string DeleteExecutionTokenQuery = @"
DELETE CSQ FROM [Taskling].[CriticalSectionQueue] CSQ
JOIN [Taskling].[Task] T ON CSQ.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE CST FROM [Taskling].[CriticalSectionToken] CST
JOIN [Taskling].[Task] T ON CST.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE ET FROM [Taskling].[ExecutionToken] ET
JOIN [Taskling].[Task] T ON ET.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE TE FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[Task] T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE FROM [Taskling].[Task] 
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Delete All :.

        #region .: Tasks :.

        private const string InsertTaskQuery = @"INSERT INTO [Taskling].[Task]([ApplicationName],[TaskName])
VALUES(@ApplicationName,@TaskName);

SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskSecondaryId]
  FROM [Taskling].[Task]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Tasks :.

        #region .: Keep Alive :.

        private const string SetKeepAliveQuery = @"UPDATE [Taskling].[ExecutionToken]
SET [LastKeepAlive] = @KeepAliveDateTime
WHERE [TaskExecutionId] = @TaskExecutionId;

UPDATE [Taskling].[TaskExecution]
SET [LastKeepAlive] = @KeepAliveDateTime
WHERE [TaskExecutionId] = @TaskExecutionId";

        #endregion .: Keep Alive :.
        
        #region .: Task Executions :.

        private const string InsertTaskExecutionQuery = @"INSERT INTO [Taskling].[TaskExecution]
           ([TaskSecondaryId]
           ,[StartedAt]
           ,[LastKeepAlive])
     VALUES
           (@TaskSecondaryId
           ,GETUTCDATE()
           ,GETUTCDATE());

SELECT CAST(SCOPE_IDENTITY() AS INT);
";

        #endregion .: Task Executions :.

        #region .: Critical Sections :.

        private const string InsertCriticalSectionTokenQuery = @"INSERT INTO [Taskling].[CriticalSectionToken]
           ([TaskSecondaryId]
           ,[DateGranted]
           ,[DateReturned]
           ,[Status]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskSecondaryId
           ,GETUTCDATE()
           ,NULL
           ,@Status
           ,@TaskExecutionId
           ,@TaskExecutionId)";

        private const string GetQueueCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[CriticalSectionQueue]
WHERE [TaskExecutionId] = @TaskExecutionId";

        private const string InsertIntoCriticalSectionQueueQuery = @"INSERT INTO [Taskling].[CriticalSectionQueue]
           ([TaskSecondaryId]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskSecondaryId
           ,@TaskExecutionId
           ,@TaskExecutionId)";

        private const string GetCriticalSectionTokenStatusByTaskExecutionQuery = @"SELECT CST.[Status]
FROM [Taskling].[TaskExecution] TE
JOIN [Taskling].[CriticalSectionToken] CST ON TE.TaskExecutionId = CST.TaskExecutionId
JOIN [Taskling].[Task] T ON CST.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Critical Sections :.

        #endregion .: Queries :.


        public void DeleteRecordsOfTask(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = DeleteExecutionTokenQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.ExecuteNonQuery();
            }
        }

        public void SetKeepAlive(int taskExecutionId)
        {
            SetKeepAlive(taskExecutionId, DateTime.UtcNow);
        }

        public void SetKeepAlive(int taskExecutionId, DateTime keepAliveDateTime)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = SetKeepAliveQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@KeepAliveDateTime", SqlDbType.DateTime).Value = keepAliveDateTime;
                
                command.ExecuteNonQuery();
            }
        }


        #region .: Tasks :.

        //public int CreateTaskAndExecutionToken(string applicationName, string taskName, int tokenCount = 1)
        //{
        //    var taskSecondaryId = InsertTask(applicationName, taskName);
        //    InsertUnavailableExecutionToken(taskSecondaryId, 0);

        //    return taskSecondaryId;
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
                    return int.Parse(reader["TaskSecondaryId"].ToString());
                }
            }

            return -1;
        }

        #endregion .: Tasks :.


        #region .: Execution Tokens :.

        public void InsertUnavailableExecutionToken(int taskSecondaryId)
        {
            InsertExecutionToken(taskSecondaryId, TaskExecutionStatus.Unavailable, 0);
        }

        public void InsertAvailableExecutionToken(int taskSecondaryId)
        {
            InsertExecutionToken(taskSecondaryId, TaskExecutionStatus.Available, 0);
        }

        public void InsertUnlimitedExecutionToken(int taskSecondaryId)
        {
            InsertExecutionToken(taskSecondaryId, TaskExecutionStatus.Unlimited, 0);
        }

        public void InsertExecutionToken(int taskSecondaryId, TaskExecutionStatus status, int taskExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertExecutionTokenQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@Status", SqlDbType.Int).Value = (int)status;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.ExecuteNonQuery();
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

        public int InsertTaskExecution(int taskSecondaryId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertTaskExecutionQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                return (int)command.ExecuteScalar();
            }
        }

        #endregion .: Task Executions :.


        #region .: Critical Sections :.

        public void InsertUnavailableCriticalSectionToken(int taskSecondaryId, int taskExecutionId)
        {
            InsertCriticalSectionToken(taskSecondaryId, taskExecutionId, 0);
        }

        public void InsertAvailableCriticalSectionToken(int taskSecondaryId, int taskExecutionId)
        {
            InsertCriticalSectionToken(taskSecondaryId, taskExecutionId, 1);
        }

        private void InsertCriticalSectionToken(int taskSecondaryId, int taskExecutionId, byte status)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertCriticalSectionTokenQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = status;
                command.ExecuteNonQuery();
            }
        }

        public int GetQueueCount(int taskExecutionId)
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

        public void InsertIntoCriticalSectionQueue(int taskSecondaryId, int taskExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertIntoCriticalSectionQueueQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
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
