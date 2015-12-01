using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Blocks;
using Taskling.InfrastructureContracts.Blocks;

namespace Taskling.SqlServer.IntegrationTest.TestHelpers
{
    public class BlocksHelper
    {
        #region .: Queries :.

        private const string InsertDateRangeBlockQuery = @"INSERT INTO [Taskling].[DateRangeBlock]
           ([TaskSecondaryId]
           ,[FromDate]
           ,[ToDate]
           ,[CreatedDate])
     VALUES
           (@TaskSecondaryId
           ,@FromDate
           ,@ToDate
           ,@FromDate);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertNumericRangeBlockQuery = @"INSERT INTO [Taskling].[NumericRangeBlock]
           ([TaskSecondaryId]
           ,[FromNumber]
           ,[ToNumber]
           ,[CreatedDate])
     VALUES
           (@TaskSecondaryId
           ,@FromNumber
           ,@ToNumber
           ,@CreatedDate);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertListBlockQuery = @"INSERT INTO [Taskling].[ListBlock]
           ([TaskSecondaryId]
           ,[CreatedDate])
     VALUES
           (@TaskSecondaryId
           ,@CreatedDate);

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

        private const string InsertListBlockExecutionQuery = @"INSERT INTO [Taskling].[ListBlockExecution]
           ([TaskExecutionId]
           ,[ListBlockId]
           ,[StartedAt]
           ,[CompletedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@ListBlockId
           ,@StartedAt
           ,@CompletedAt
           ,@BlockExecutionStatus);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string DeleteBlocksQuery =
            @"
DELETE DRBE FROM [Taskling].[DateRangeBlockExecution] DRBE
LEFT JOIN [Taskling].[TaskExecution] TE ON DRBE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN [Taskling].[Task] T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL
OR TE.TaskExecutionId IS NULL;

DELETE DRBE FROM [Taskling].[DateRangeBlock] DRBE
LEFT JOIN [Taskling].[Task] T ON DRBE.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL;

DELETE NRBE FROM [Taskling].[NumericRangeBlockExecution] NRBE
LEFT JOIN [Taskling].[TaskExecution] TE ON NRBE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN [Taskling].[Task] T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL
OR TE.TaskExecutionId IS NULL;

DELETE NRBE FROM [Taskling].[NumericRangeBlock] NRBE
LEFT JOIN [Taskling].[Task] T ON NRBE.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL;

DELETE LBE FROM [Taskling].[ListBlockExecution] LBE
LEFT JOIN [Taskling].[ListBlock] LB ON LBE.ListBlockId = LB.ListBlockId
LEFT JOIN [Taskling].[Task] T ON LB.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL
OR LB.ListBlockId IS NULL;

DELETE LBI FROM [Taskling].[ListBlockItem] LBI
LEFT JOIN [Taskling].[ListBlock] LB ON LBI.ListBlockId = LB.ListBlockId 
LEFT JOIN [Taskling].[Task] T ON LB.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL
OR LB.ListBlockId IS NULL;

DELETE LB FROM [Taskling].[ListBlock] LB
LEFT JOIN [Taskling].[Task] T ON LB.TaskSecondaryId = T.TaskSecondaryId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskSecondaryId IS NULL;";

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

        private const string GetListBlockCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[ListBlock] LB
JOIN [Taskling].[Task]  T ON LB.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;";

        private const string GetListBlockExecutionsCountByStatusQuery = @"SELECT COUNT(*)
FROM [TasklingDb].[Taskling].[ListBlockExecution] LBE
JOIN [Taskling].[TaskExecution] TE ON LBE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[Task]  T ON TE.TaskSecondaryId = T.TaskSecondaryId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName
AND LBE.BlockExecutionStatus = @BlockExecutionStatus;";

        private const string GetListBlockItemCountByStatusQuery = @"SELECT COUNT(*)
FROM [Taskling].[ListBlockItem] LBI
WHERE LBI.ListBlockId = @ListBlockId
AND LBI.Status = @Status;";

        #endregion .: Queries :.

        #region .: Insert and Delete Blocks :.

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

        public long InsertNumericRangeBlock(int taskSecondaryId, long fromNumber, long toNumber, DateTime createdDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertNumericRangeBlockQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = fromNumber;
                command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = toNumber;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertListBlock(int taskSecondaryId, DateTime createdDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertListBlockQuery;
                command.Parameters.Add("@TaskSecondaryId", SqlDbType.Int).Value = taskSecondaryId;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertDateRangeBlockExecution(string taskExecutionId, long dateRangeBlockId, DateTime startedAt, DateTime completedAt, BlockExecutionStatus executionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertDateRangeBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);
                command.Parameters.Add("@DateRangeBlockId", SqlDbType.BigInt).Value = dateRangeBlockId;
                command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = startedAt;
                command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = completedAt;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)executionStatus;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertNumericRangeBlockExecution(string taskExecutionId, long numericRangeBlockId, DateTime startedAt, DateTime completedAt, BlockExecutionStatus executionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertNumericRangeBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);
                command.Parameters.Add("@NumericRangeBlockId", SqlDbType.BigInt).Value = numericRangeBlockId;
                command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = startedAt;
                command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = completedAt;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)executionStatus;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertListBlockExecution(int taskExecutionId, long listBlockId, DateTime startedAt, DateTime completedAt, BlockExecutionStatus executionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertListBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = taskExecutionId;
                command.Parameters.Add("@ListBlockId", SqlDbType.BigInt).Value = listBlockId;
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

        #endregion .: Insert and Delete Blocks :.

        #region .: Get Block Counts :.

        public int GetDateRangeBlockCount(string applicationName, string taskName)
        {
            return GetBlockCount(applicationName, taskName, GetDateRangeBlockCountQuery);
        }

        public int GetNumericRangeBlockCount(string applicationName, string taskName)
        {
            return GetBlockCount(applicationName, taskName, GetNumericRangeBlockCountQuery);
        }

        public int GetListBlockCount(string applicationName, string taskName)
        {
            return GetBlockCount(applicationName, taskName, GetListBlockCountQuery);
        }

        private int GetBlockCount(string applicationName, string taskName, string query)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                return (int)command.ExecuteScalar();
            }
        }

        #endregion .: Get Block Counts :.

        #region .: Get Block Execution Counts :.

        public int GetDateRangeBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus)
        {
            return GetBlockExecutionCountByStatus(applicationName, taskName, blockExecutionStatus, GetDateRangeBlockExecutionsCountByStatusQuery);
        }

        public int GetNumericRangeBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus)
        {
            return GetBlockExecutionCountByStatus(applicationName, taskName, blockExecutionStatus, GetNumericRangeBlockExecutionsCountByStatusQuery);
        }

        public int GetListBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus)
        {
            return GetBlockExecutionCountByStatus(applicationName, taskName, blockExecutionStatus, GetListBlockExecutionsCountByStatusQuery);
        }

        private int GetBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus, string query)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = query;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)blockExecutionStatus;
                return (int)command.ExecuteScalar();
            }
        }

        #endregion .: Get Block Execution Counts :.

        public int GetListBlockItemCountByStatus(string listBlockId, ListBlockItemStatus status)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetListBlockItemCountByStatusQuery;
                command.Parameters.Add("@ListBlockId", SqlDbType.BigInt).Value = long.Parse(listBlockId);
                command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = (byte)status;
                return (int)command.ExecuteScalar();
            }
        }

        
    }
}
