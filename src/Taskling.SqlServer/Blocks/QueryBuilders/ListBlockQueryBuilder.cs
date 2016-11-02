using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks.QueryBuilders
{
    internal class ListBlockQueryBuilder
    {
        public const string InsertListBlock = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[BlockType]
           ,[CreatedDate]
           ,[ObjectData]
           ,[CompressedObjectData])
     VALUES
           (@TaskDefinitionId
           ,@BlockType
           ,GETUTCDATE()
           ,@ObjectData
           ,@CompressedObjectData);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        public const string GetListBlockItems = @"SELECT [ListBlockItemId]
      ,[BlockId]
      ,[Value]
      ,[CompressedValue]
      ,[Status]
      ,[LastUpdated]
      ,[StatusReason]
      ,[Step]
FROM [Taskling].[ListBlockItem]
WHERE [BlockId] = @BlockId";

        public const string UpdateSingleBlockListItemStatus = @"UPDATE [Taskling].[ListBlockItem]
   SET [Status] = @Status
        ,[StatusReason] = @StatusReason
        ,[Step] = @Step
        ,[LastUpdated] = GETUTCDATE()
WHERE BlockId = @BlockId
AND ListBlockItemId = @ListBlockItemId";

        private const string CreateTemporaryTable = @"CREATE TABLE {0}(
    [ListBlockItemId] bigint NOT NULL,
    [BlockId] bigint NOT NULL,
    [Status] tinyint NOT NULL,
    [StatusReason] nvarchar(max) NULL,
    [Step] tinyint NULL);";

        private const string BulkUpdateBlockListItemStatus = @"UPDATE LBI
   SET [Status] = T.[Status]
    ,[StatusReason] = T.[StatusReason]
    ,[Step] = T.[Step]
    ,[LastUpdated] = GETUTCDATE()
FROM [Taskling].[ListBlockItem] LBI
JOIN {0} AS T ON LBI.BlockId = T.BlockId
	AND LBI.ListBlockItemId = T.ListBlockItemId";

        public const string GetLastListBlock = @"
SELECT TOP 1 [BlockId]
      ,[TaskDefinitionId]
      ,[FromDate]
      ,[ToDate]
      ,[FromNumber]
      ,[ToNumber]
      ,[BlockType]
      ,[CreatedDate]
      ,[ObjectData]
      ,[CompressedObjectData]
FROM [Taskling].[Block]
WHERE [TaskDefinitionId] = @TaskDefinitionId 
AND [IsPhantom] = 0
ORDER BY [BlockId] DESC";

        public static string GetCreateTemporaryTableQuery(string tableName)
        {
            return string.Format(CreateTemporaryTable, tableName);
        }

        public static string GetBulkUpdateBlockListItemStatus(string tableName)
        {
            return string.Format(BulkUpdateBlockListItemStatus, tableName);
        }

    }
}
