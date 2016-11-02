using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks.QueryBuilders
{
    public class ObjectBlockQueryBuilder
    {
        public const string GetLastObjectBlock = @"
SELECT TOP 1 [BlockId]
      ,[TaskDefinitionId]
      ,[ObjectData]
      ,[CompressedObjectData]
      ,[BlockType]
      ,[CreatedDate]
FROM [Taskling].[Block]
WHERE [TaskDefinitionId] = @TaskDefinitionId 
AND [IsPhantom] = 0
ORDER BY [BlockId] DESC";

        public const string InsertObjectBlock = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[ObjectData]
           ,[CompressedObjectData]
           ,[BlockType]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@ObjectData
           ,@CompressedObjectData
           ,@BlockType
           ,GETUTCDATE());

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";
    }
}
