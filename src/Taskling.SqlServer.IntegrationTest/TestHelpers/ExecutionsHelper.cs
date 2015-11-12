using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

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

        private const string InsertQuery = @"
        INSERT INTO [PC].[ExecutionTokens]
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
           ,NULL
           ,NULL)";

        private const string DeleteQuery = @"DELETE ET FROM [PC].[ExecutionTokens] ET
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

        private const string InsertTaskQuery = @"INSERT INTO PC.[Task]([ApplicationName],[TaskName])
VALUES(@ApplicationName,@TaskName);

SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskSecondaryId]
  FROM [PC].[Task]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        private const string SetKeepAliveQuery = @"UPDATE PC.[ExecutionTokens]
SET [LastKeepAlive] = GETDATE()
WHERE [TaskExecutionId] = @TaskExecutionId";

        private const string GetTokenStatusByTaskExecutionQuery = @"SELECT ET.[Status]
FROM [PC].[TaskExecution] TE
JOIN [PC].[ExecutionTokens] ET ON TE.TaskExecutionId = ET.TaskExecutionId
JOIN [PC].[Task] T ON ET.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName";

        #endregion .: Queries :.

        public void InsertExecutionToken(int taskSecondaryId, int taskExecutionId, int status)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@Status", SqlDbType.Int).Value = status;
                command.ExecuteNonQuery();
            }
        }

        public void InsertExecutionToken(int taskSecondaryId, int taskExecutionId)
        {
            InsertExecutionToken(taskSecondaryId, taskExecutionId, 1);
        }

        public void DeleteRecordsOfTask(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = DeleteQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.ExecuteNonQuery();
            }
        }

        public int CreateTaskAndExecutionToken(string applicationName, string taskName, int tokenCount = 1)
        {
            var taskSecondaryId = InsertTask(applicationName, taskName);
            for(int i=0; i<tokenCount; i++)
                InsertExecutionToken(taskSecondaryId, 0, 1);

            return taskSecondaryId;
        }

        private int InsertTask(string applicationName, string taskName)
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

        public byte GetTokenStatus(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetTokenStatusByTaskExecutionQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                return (byte) command.ExecuteScalar();
            }
        }
    }
}
