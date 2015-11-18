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
        private readonly string ConnectionString;

        public ExecutionsHelper(string connectionString)
        {
            ConnectionString = connectionString;
        }

        #region .: Queries :.

        #region .: Execution Tokens :.

        private const string InsertExecutionTokenQuery = @"
        INSERT INTO [PC].[ExecutionTokens]
           ([TaskSecondaryId]
           ,[DateGranted]
           ,[DateReturned]
           ,[Status]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId]
           ,[LastKeepAlive])
     VALUES
           (@TaskSecondaryId
           ,GETDATE()
           ,NULL
           ,@Status
           ,@TaskExecutionId
           ,@TaskExecutionId
           ,GETDATE());";

        private const string GetExecutionTokenStatusByTaskExecutionQuery = @"SELECT ET.[Status]
FROM [PC].[TaskExecution] TE
JOIN [PC].[ExecutionTokens] ET ON TE.TaskExecutionId = ET.TaskExecutionId
JOIN [PC].[Task] T ON ET.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Execution Tokens :.

        #region .: Delete All :.

        private const string DeleteExecutionTokenQuery = @"
DELETE CSQ FROM [PC].[CriticalSectionQueue] CSQ
JOIN [PC].[Task] T ON CSQ.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE CST FROM [PC].[CriticalSectionTokens] CST
JOIN [PC].[Task] T ON CST.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE ET FROM [PC].[ExecutionTokens] ET
JOIN [PC].[Task] T ON ET.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE TE FROM [PC].[TaskExecution] TE
JOIN [PC].[Task] T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE FROM [PC].[Task] 
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Delete All :.

        #region .: Tasks :.

        private const string InsertTaskQuery = @"INSERT INTO PC.[Task]([ApplicationName],[TaskName])
VALUES(@ApplicationName,@TaskName);

SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskSecondaryId]
  FROM [PC].[Task]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Tasks :.

        #region .: Keep Alive :.

        private const string SetKeepAliveQuery = @"UPDATE PC.[ExecutionTokens]
SET [LastKeepAlive] = GETDATE()
WHERE [TaskExecutionId] = @TaskExecutionId";

        #endregion .: Keep Alive :.
        
        #region .: Task Executions :.

        private const string InsertTaskExecutionQuery = @"INSERT INTO [PC].[TaskExecution]
           ([TaskSecondaryId]
           ,[StartedAt])
     VALUES
           (@TaskSecondaryId
           ,GETDATE());

SELECT CAST(SCOPE_IDENTITY() AS INT);
";

        #endregion .: Task Executions :.

        #region .: Critical Sections :.

        private const string InsertCriticalSectionTokenQuery = @"INSERT INTO [PC].[CriticalSectionTokens]
           ([TaskSecondaryId]
           ,[DateGranted]
           ,[DateReturned]
           ,[Status]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskSecondaryId
           ,GETDATE()
           ,NULL
           ,@Status
           ,@TaskExecutionId
           ,@TaskExecutionId)";

        private const string GetQueueCountQuery = @"SELECT COUNT(*)
FROM [PC].[CriticalSectionQueue]
WHERE [TaskExecutionId] = @TaskExecutionId";

        private const string InsertIntoCriticalSectionQueueQuery = @"INSERT INTO [PC].[CriticalSectionQueue]
           ([TaskSecondaryId]
           ,[TaskExecutionId]
           ,[HoldLockTaskExecutionId])
     VALUES
           (@TaskSecondaryId
           ,@TaskExecutionId
           ,@TaskExecutionId)";

        private const string GetCriticalSectionTokenStatusByTaskExecutionQuery = @"SELECT CST.[Status]
FROM [PC].[TaskExecution] TE
JOIN [PC].[CriticalSectionTokens] CST ON TE.TaskExecutionId = CST.TaskExecutionId
JOIN [PC].[Task] T ON CST.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Critical Sections :.

        #endregion .: Queries :.


        public void DeleteRecordsOfTask(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = SetKeepAliveQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
            using (var connection = new SqlConnection(ConnectionString))
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
