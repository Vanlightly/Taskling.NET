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

        private const string InsertDateRangeBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromDate]
           ,[ToDate]
           ,[CreatedDate]
           ,[BlockType])
     VALUES
           (@TaskDefinitionId
           ,@FromDate
           ,@ToDate
           ,@FromDate
           ,@BlockType);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertNumericRangeBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromNumber]
           ,[ToNumber]
           ,[CreatedDate]
           ,[BlockType])
     VALUES
           (@TaskDefinitionId
           ,@FromNumber
           ,@ToNumber
           ,@CreatedDate
           ,@BlockType);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertListBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[CreatedDate]
           ,[BlockType])
     VALUES
           (@TaskDefinitionId
           ,@CreatedDate
           ,@BlockType);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertBlockExecutionQuery = @"INSERT INTO [Taskling].[BlockExecution]
           ([TaskExecutionId]
           ,[BlockId]
           ,[StartedAt]
           ,[CompletedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@BlockId
           ,@StartedAt
           ,@CompletedAt
           ,@BlockExecutionStatus);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string DeleteBlocksQuery =
            @"
DELETE BE FROM [Taskling].[BlockExecution] BE
LEFT JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskDefinitionId IS NULL
OR TE.TaskExecutionId IS NULL;

DELETE B FROM [Taskling].[Block] B
LEFT JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskDefinitionId IS NULL;

DELETE LBI FROM [Taskling].[ListBlockItem] LBI
LEFT JOIN [Taskling].[Block] B ON LBI.BlockId = B.BlockId 
LEFT JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
WHERE (T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName)
OR T.TaskDefinitionId IS NULL
OR B.BlockId IS NULL;";

        private const string GetBlockCountQuery = @"SELECT COUNT(*)
FROM [Taskling].[Block] B
JOIN [Taskling].[TaskDefinition]  T ON B.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName;";

        private const string GetBlockExecutionsCountByStatusQuery = @"SELECT COUNT(*)
FROM [Taskling].[BlockExecution] BE
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN [Taskling].[TaskDefinition]  T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE T.ApplicationName = @ApplicationName
AND T.TaskName = @TaskName
AND BE.BlockExecutionStatus = @BlockExecutionStatus;";

        private const string GetListBlockItemCountByStatusQuery = @"SELECT COUNT(*)
FROM [Taskling].[ListBlockItem] LBI
WHERE LBI.BlockId = @BlockId
AND LBI.Status = @Status;";

        #endregion .: Queries :.

        #region .: Insert and Delete Blocks :.

        public long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertDateRangeBlockQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = fromDate;
                command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = toDate;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.DateRange;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertNumericRangeBlock(int taskDefinitionId, long fromNumber, long toNumber, DateTime createdDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertNumericRangeBlockQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = fromNumber;
                command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = toNumber;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.NumericRange;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertListBlock(int taskDefinitionId, DateTime createdDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertListBlockQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.List;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertBlockExecution(string taskExecutionId, long blockId, DateTime startedAt, DateTime completedAt, BlockExecutionStatus executionStatus)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);
                command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
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

        public int GetBlockCount(string applicationName, string taskName)
        {
            return GetBlockCount(applicationName, taskName, GetBlockCountQuery);
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

        public int GetBlockExecutionCountByStatus(string applicationName, string taskName, BlockExecutionStatus blockExecutionStatus)
        {
            return GetBlockExecutionCountByStatus(applicationName, taskName, blockExecutionStatus, GetBlockExecutionsCountByStatusQuery);
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

        public int GetListBlockItemCountByStatus(string blockId, ListBlockItemStatus status)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetListBlockItemCountByStatusQuery;
                command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = long.Parse(blockId);
                command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = (byte)status;
                return (int)command.ExecuteScalar();
            }
        }

        
    }
}
