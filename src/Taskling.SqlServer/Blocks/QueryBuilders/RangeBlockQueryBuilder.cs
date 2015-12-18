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

        
    }
}
