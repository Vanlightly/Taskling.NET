using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.SqlServer.IntegrationTest.TestHelpers
{
    public class BlocksHelper
    {
        #region .: Queries :.

        private const string InsertDateRangeBlockQuery = @"INSERT INTO [Taskling].[DateRangeBlock]
           ([TaskSecondaryId]
           ,[FromDate]
           ,[ToDate])
     VALUES
           (@TaskSecondaryId
           ,@FromDate
           ,@ToDate);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertNumericRangeBlockQuery = @"INSERT INTO [Taskling].[NumericRangeBlock]
           ([TaskSecondaryId]
           ,[FromNumber]
           ,[ToNumber])
     VALUES
           (@TaskSecondaryId
           ,@FromNumber
           ,@ToNumber);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertDateRangeBlockExecutionQuery = @"INSERT INTO [Taskling].[DateRangeBlockExecution]
           ([TaskExecutionId]
           ,[DateRangeBlockId]
           ,[StartedAt]
           ,[CompletedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@DateRangeBlockId
           ,@StartedAt
           ,@CompletedAt
           ,@BlockExecutionStatus);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertNumericRangeBlockExecutionQuery = @"INSERT INTO [Taskling].[NumericRangeBlockExecution]
           ([TaskExecutionId]
           ,[NumericRangeBlockId]
           ,[StartedAt]
           ,[CompletedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@NumericRangeBlockId
           ,@StartedAt
           ,@CompletedAt
           ,@BlockExecutionStatus);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string DeleteBlocksQuery =
            @"
DELETE DRBE FROM [Taskling].[DateRangeBlockExecution] DRBE
JOIN [Taskling].[TaskExecution] TE ON DRBE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[Task] T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE DRBE FROM [Taskling].[DateRangeBlock] DRBE
JOIN [Taskling].[Task] T ON DRBE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE NRBE FROM [Taskling].[NumericRangeBlockExecution] NRBE
JOIN [Taskling].[TaskExecution] TE ON NRBE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[Task] T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;

DELETE NRBE FROM [Taskling].[NumericRangeBlock] NRBE
JOIN [Taskling].[Task] T ON NRBE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;";

        private const string GetDateRangeBlockCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[DateRangeBlock] DRB
JOIN [Taskling].[Task]  T ON DRB.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;";

        private const string GetDateRangeBlockExecutionsCountByStatusQuery = @"SELECT COUNT(*)
FROM [TasklingDb].[Taskling].[DateRangeBlockExecution] DRBE
JOIN [Taskling].[TaskExecution] TE ON DRBE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[Task]  T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName
AND DRBE.BlockExecutionStatus = @BlockExecutionStatus;";

        private const string GetNumericRangeBlockCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[NumericRangeBlock] NRB
JOIN [Taskling].[Task]  T ON NRB.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;";

        private const string GetNumericRangeBlockExecutionsCountByStatusQuery = @"SELECT COUNT(*)
FROM [TasklingDb].[Taskling].[NumericRangeBlockExecution] NRBE
JOIN [Taskling].[TaskExecution] TE ON NRBE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[Task]  T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName
AND NRBE.BlockExecutionStatus = @BlockExecutionStatus;";

        #endregion .: Queries :.

        public long InsertDateRangeBlock(int taskSecondaryId, DateTime fromDate, DateTime toDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertDateRangeBlockQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = fromDate;
                command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = toDate;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertNumericRangeBlock(int taskSecondaryId, long fromNumber, long toNumber)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertNumericRangeBlockQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = fromNumber;
                command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = toNumber;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertDateRangeBlockExecution(int taskExecutionId, long dateRangeBlockId, DateTime startedAt, DateTime completedAt, BlockExecutionStatus executionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertDateRangeBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@DateRangeBlockId", SqlDbType.BigInt).Value = dateRangeBlockId;
                command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = startedAt;
                command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = completedAt;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)executionStatus;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertNumericRangeBlockExecution(int taskExecutionId, long numericRangeBlockId, DateTime startedAt, DateTime completedAt, BlockExecutionStatus executionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertNumericRangeBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@NumericRangeBlockId", SqlDbType.BigInt).Value = numericRangeBlockId;
                command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = startedAt;
                command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = completedAt;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)executionStatus;
                return (long)command.ExecuteScalar();
            }
        }

        public void DeleteBlocks(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = DeleteBlocksQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.ExecuteNonQuery();
            }
        }

        public int GetDateRangeBlockCount(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetDateRangeBlockCountQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                return (int)command.ExecuteScalar();
            }
        }

        public int GetNumericRangeBlockCount(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetNumericRangeBlockCountQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                return (int)command.ExecuteScalar();
            }
        }

        public int GetDateRangeBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetDateRangeBlockExecutionsCountByStatusQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)blockExecutionStatus;
                return (int)command.ExecuteScalar();
            }
        }

        public int GetNumericRangeBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetNumericRangeBlockExecutionsCountByStatusQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)blockExecutionStatus;
                return (int)command.ExecuteScalar();
            }
        }
    }
}
