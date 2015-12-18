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
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@BlockType
           ,GETUTCDATE());

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        public const string GetListBlockItems = @"SELECT [ListBlockItemId]
      ,[BlockId]
      ,[Value]
      ,[Status]
FROM [Taskling].[ListBlockItem]
WHERE [BlockId] = @BlockId";

        public const string UpdateSingleBlockListItemStatus = @"UPDATE [Taskling].[ListBlockItem]
   SET [Status] =@Status
WHERE BlockId = @BlockId
AND ListBlockItemId = @ListBlockItemId";

        private const string CreateTemporaryTable = @"CREATE TABLE {0}(
    [ListBlockItemId] bigint NOT NULL,
    [BlockId] bigint NOT NULL,
    [Status] tinyint NOT NULL);";

        private const string BulkUpdateBlockListItemStatus = @"UPDATE LBI
   SET [Status] =T.[Status]
FROM [Taskling].[ListBlockItem] LBI
JOIN {0} AS T ON LBI.BlockId = T.BlockId
	AND LBI.ListBlockItemId = T.ListBlockItemId";

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
