using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class RangeBlockQueryBuilder
    {
        public const string InsertDateRangeBlock = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromDate]
           ,[ToDate]
           ,[BlockType]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@FromDate
           ,@ToDate
           ,@BlockType
           ,GETUTCDATE());

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        public const string InsertNumericRangeBlock = @"INSERT INTO [Taskling].[Block]
           ([TaskDefinitionId]
           ,[FromNumber]
           ,[ToNumber]
           ,[BlockType]
           ,[CreatedDate])
     VALUES
           (@TaskDefinitionId
           ,@FromNumber
           ,@ToNumber
           ,@BlockType
           ,GETUTCDATE());

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        public const string InsertBlockExecution = @"INSERT INTO [Taskling].[BlockExecution]
           ([TaskExecutionId]
           ,[BlockId]
           ,[StartedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@BlockId
           ,GETUTCDATE()
           ,0);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        public const string GetLastDateRangeBlock = @"
SELECT TOP 1 [BlockId]
      ,[TaskDefinitionId]
      ,[FromDate]
      ,[ToDate]
      ,[FromNumber]
      ,[ToNumber]
      ,[BlockType]
      ,[CreatedDate]
FROM [Taskling].[Block]
WHERE [TaskDefinitionId] = @TaskDefinitionId 
ORDER BY [ToDate] DESC";

        public const string GetLastNumericRangeBlock = @"
SELECT TOP 1 [BlockId]
      ,[TaskDefinitionId]
      ,[FromDate]
      ,[ToDate]
      ,[FromNumber]
      ,[ToNumber]
      ,[BlockType]
      ,[CreatedDate]
FROM [Taskling].[Block]
WHERE [TaskDefinitionId] = @TaskDefinitionId 
ORDER BY [ToNumber] DESC";
    }
}
