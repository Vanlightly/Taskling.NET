using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks.QueryBuilders
{
    internal class ListBlockQueryBuilder
    {
        private const string InsertListBlock = @"INSERT INTO {0}.[ListBlock]
           ([TaskSecondaryId]
           ,[CreatedDate])
     VALUES
           (@TaskSecondaryId
           ,GETUTCDATE());

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertListBlockExecution = @"INSERT INTO {0}.[ListBlockExecution]
           ([TaskExecutionId]
           ,[ListBlockId]
           ,[StartedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@ListBlockId
           ,GETUTCDATE()
           ,0);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string GetListBlockItems = @"SELECT [ListBlockItemId]
      ,[ListBlockId]
      ,[Value]
      ,[Status]
FROM {0}.[ListBlockItem]
WHERE [ListBlockId] = @ListBlockId";

        private const string UpdateSingleBlockListItemStatus = @"UPDATE {0}.[ListBlockItem]
   SET [Status] =@Status
WHERE ListBlockId = @ListBlockId
AND ListBlockItemId = @ListBlockItemId";

        private const string CreateTemporaryTable = @"CREATE TABLE {0}(
    [ListBlockItemId] bigint NOT NULL,
    [ListBlockId] bigint NOT NULL,
    [Status] tinyint NOT NULL);";

        private const string BulkUpdateBlockListItemStatus = @"UPDATE LBI
   SET [Status] =T.[Status]
FROM {0}.[ListBlockItem] LBI
JOIN {1} AS T ON LBI.ListBlockId = T.ListBlockId
	AND LBI.ListBlockItemId = T.ListBlockItemId";

        public static string GetInsertListBlockQuery(string tableSchema)
        {
            return string.Format(InsertListBlock, tableSchema);
        }

        public static string GetInsertListBlockExecutionQuery(string tableSchema)
        {
            return string.Format(InsertListBlockExecution, tableSchema);
        }

        public static string GetListBlockItemsQuery(string tableSchema)
        {
            return string.Format(GetListBlockItems, tableSchema);
        }

        public static string GetCreateTemporaryTableQuery(string tableName)
        {
            return string.Format(CreateTemporaryTable, tableName);
        }

        public static string GetBulkUpdateBlockListItemStatus(string tableSchema, string tableName)
        {
            return string.Format(BulkUpdateBlockListItemStatus, tableSchema, tableName);
        }

        public static string GetUpdateSingleBlockListItemStatus(string tableSchema)
        {
            return string.Format(UpdateSingleBlockListItemStatus, tableSchema);
        }
    }
}
