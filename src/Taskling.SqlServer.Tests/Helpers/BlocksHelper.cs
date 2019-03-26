using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;
using Taskling.Blocks.ListBlocks;
using Taskling.Serialization;

namespace Taskling.SqlServer.Tests.Helpers
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
           ,@CreatedDate
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
           ,[BlockType]
           ,[ObjectData])
     VALUES
           (@TaskDefinitionId
           ,@CreatedDate
           ,@BlockType
           ,@ObjectData);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertObjectBlockQuery = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[CreatedDate]
           ,[BlockType]
           ,[ObjectData])
     VALUES
           (@TaskDefinitionId
           ,@CreatedDate
           ,@BlockType
           ,@ObjectData);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertBlockExecutionQuery = @"INSERT INTO [Taskling].[BlockExecution]
           ([TaskExecutionId]
           ,[BlockId]
           ,[CreatedAt]
           ,[StartedAt]
           ,[CompletedAt]
           ,[BlockExecutionStatus]
           ,[Attempt])
     VALUES
           (@TaskExecutionId
           ,@BlockId
           ,@CreatedAt
           ,@StartedAt
           ,@CompletedAt
           ,@BlockExecutionStatus
           ,@Attempt);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string DeleteBlocksQuery =
            @"
DELETE BE FROM [Taskling].[BlockExecution] BE
LEFT JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId
WHERE (T.ApplicationName = @ApplicationName)
OR T.TaskDefinitionId IS NULL
OR TE.TaskExecutionId IS NULL;

DELETE B FROM [Taskling].[Block] B
LEFT JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
WHERE (T.ApplicationName = @ApplicationName)
OR T.TaskDefinitionId IS NULL;

DELETE LBI FROM [Taskling].[ListBlockItem] LBI
LEFT JOIN [Taskling].[Block] B ON LBI.BlockId = B.BlockId 
LEFT JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId
WHERE (T.ApplicationName = @ApplicationName)
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

        private const string GetItemsCountQuery = @"SELECT [ItemsCount]
FROM [Taskling].[BlockExecution]
WHERE [BlockExecutionId] = @BlockExecutionId";

        private const string GetLastBlockIdQuery = @"SELECT MAX(BlockId)
FROM [Taskling].[Block] B
JOIN [Taskling].[TaskDefinition] TD ON B.TaskDefinitionId = TD.TaskDefinitionId
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        private const string GetListBlockItemsQuery = @"SELECT [ListBlockItemId]
      ,[Value]
      ,[Status]
      ,[LastUpdated]
      ,[StatusReason]
      ,[Step]
FROM [Taskling].[ListBlockItem]
WHERE [BlockId] = @BlockId
AND [Status] = @Status";

        private const string InsertForcedBlockQueueQuery = @"INSERT INTO [Taskling].[ForceBlockQueue]
           ([BlockId]
           ,[ForcedBy]
           ,[ForcedDate]
           ,[ProcessingStatus])
     VALUES
           (@BlockId
           ,'Test'
           ,GETUTCDATE()
           ,'Pending')";

        private const string InsertPhantomNumericBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromNumber]
           ,[ToNumber]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@FromNumber
           ,@ToNumber
           ,@BlockType
           ,1
           ,GETUTCDATE())";

        private const string InsertPhantomDateBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromDate]
           ,[ToDate]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@FromDate
           ,@ToDate
           ,@BlockType
           ,1
           ,GETUTCDATE())";

        private const string InsertPhantomListBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@BlockType
           ,1
           ,GETUTCDATE())

DECLARE @BlockId BIGINT = (SELECT CAST(SCOPE_IDENTITY() AS BIGINT))

INSERT INTO [Taskling].[ListBlockItem]
           ([BlockId]
           ,[Value]
           ,[Status])
     VALUES
           (@BlockId
           ,'test'
           ,1)";

        private const string InsertPhantomObjectBlockQuery = @"DECLARE @TaskDefinitionId INT = (
	SELECT TaskDefinitionId 
	FROM [Taskling].[TaskDefinition]
	WHERE ApplicationName = @ApplicationName
	AND TaskName = @TaskName)

INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[ObjectData]
           ,[BlockType]
           ,[IsPhantom]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@ObjectData
           ,@BlockType
           ,1
           ,GETUTCDATE())";

        #endregion .: Queries :.

        #region .: Insert and Delete Blocks :.

        public long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate)
        {
            return InsertDateRangeBlock(taskDefinitionId, fromDate, toDate, fromDate);
        }

        public long InsertDateRangeBlock(int taskDefinitionId, DateTime fromDate, DateTime toDate, DateTime createdAt)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertDateRangeBlockQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = fromDate;
                command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = toDate;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdAt;
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

        public long InsertListBlock(int taskDefinitionId, DateTime createdDate, string objectData = null)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertListBlockQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.List;
                if (objectData == null)
                    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, 1000).Value = DBNull.Value;
                else
                    command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, 1000).Value = objectData;

                return (long)command.ExecuteScalar();
            }
        }

        public long InsertObjectBlock(int taskDefinitionId, DateTime createdDate, string objectData)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertObjectBlockQuery;
                command.Parameters.Add("@TaskDefinitionId", SqlDbType.Int).Value = taskDefinitionId;
                command.Parameters.Add("@CreatedDate", SqlDbType.DateTime).Value = createdDate;
                command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = JsonGenericSerializer.Serialize<string>(objectData);
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.Object;
                return (long)command.ExecuteScalar();
            }
        }

        public long InsertBlockExecution(string taskExecutionId, long blockId, DateTime createdAt, DateTime? startedAt, DateTime? completedAt, BlockExecutionStatus executionStatus, int attempt = 1)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertBlockExecutionQuery;
                command.Parameters.Add("@TaskExecutionId", SqlDbType.Int).Value = int.Parse(taskExecutionId);
                command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
                command.Parameters.Add("@CreatedAt", SqlDbType.DateTime).Value = createdAt;
                command.Parameters.Add("@Attempt", SqlDbType.BigInt).Value = attempt;

                if (startedAt.HasValue)
                    command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = startedAt.Value;
                else
                    command.Parameters.Add("@StartedAt", SqlDbType.DateTime).Value = DBNull.Value;

                if (completedAt.HasValue)
                    command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = completedAt.Value;
                else
                    command.Parameters.Add("@CompletedAt", SqlDbType.DateTime).Value = DBNull.Value;

                command.Parameters.Add("@BlockExecutionStatus", SqlDbType.TinyInt).Value = (byte)executionStatus;
                return (long)command.ExecuteScalar();
            }
        }

        public void DeleteBlocks(string applicationName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = DeleteBlocksQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
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

        public int GetBlockExecutionItemCount(long blockExecutionId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetItemsCountQuery;
                command.Parameters.Add("@BlockExecutionId", SqlDbType.BigInt).Value = blockExecutionId;
                var result = command.ExecuteScalar();
                return (int)result;
            }
        }

        #endregion .: Get Block Execution Counts :.

        public int GetListBlockItemCountByStatus(string blockId, ItemStatus status)
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

        public long GetLastBlockId(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetLastBlockIdQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                return (long)command.ExecuteScalar();
            }
        }

        public List<ListBlockItem<T>> GetListBlockItems<T>(string blockId, ItemStatus status)
        {
            var items = new List<ListBlockItem<T>>();

            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = GetListBlockItemsQuery;
                command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = long.Parse(blockId);
                command.Parameters.Add("@Status", SqlDbType.TinyInt).Value = (byte)status;

                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var item = new ListBlockItem<T>();
                    item.ListBlockItemId = reader.GetInt64(0).ToString();
                    item.Value = JsonGenericSerializer.Deserialize<T>(reader.GetString(1));
                    item.Status = (ItemStatus)reader.GetByte(2);

                    if (reader[4] != DBNull.Value)
                        item.StatusReason = reader.GetString(4);

                    if (reader[5] != DBNull.Value)
                        item.Step = reader.GetByte(5);

                    items.Add(item);
                }
            }

            return items;
        }

        public void EnqueueForcedBlock(long blockId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertForcedBlockQueueQuery;
                command.Parameters.Add("@BlockId", SqlDbType.BigInt).Value = blockId;
                command.ExecuteNonQuery();
            }
        }

        public void InsertPhantomDateRangeBlock(string applicationName, string taskName, DateTime fromDate, DateTime toDate)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertPhantomDateBlockQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@FromDate", SqlDbType.DateTime).Value = fromDate;
                command.Parameters.Add("@ToDate", SqlDbType.DateTime).Value = toDate;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.DateRange;
                command.ExecuteNonQuery();
            }
        }

        public void InsertPhantomNumericBlock(string applicationName, string taskName, long fromId, long toId)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertPhantomNumericBlockQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@FromNumber", SqlDbType.BigInt).Value = fromId;
                command.Parameters.Add("@ToNumber", SqlDbType.BigInt).Value = toId;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.NumericRange;
                command.ExecuteNonQuery();
            }
        }

        public void InsertPhantomListBlock(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertPhantomListBlockQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.List;
                command.ExecuteNonQuery();
            }
        }

        public void InsertPhantomObjectBlock(string applicationName, string taskName)
        {
            using (var connection = new SqlConnection(TestConstants.TestConnectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = InsertPhantomObjectBlockQuery;
                command.Parameters.Add("@ApplicationName", SqlDbType.VarChar, 200).Value = applicationName;
                command.Parameters.Add("@TaskName", SqlDbType.VarChar, 200).Value = taskName;
                command.Parameters.Add("@BlockType", SqlDbType.TinyInt).Value = (byte)BlockType.Object;
                command.Parameters.Add("@ObjectData", SqlDbType.NVarChar, -1).Value = JsonGenericSerializer.Serialize("My phantom block");
                command.ExecuteNonQuery();
            }
        }
    }
}
