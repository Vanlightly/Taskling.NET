using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Blocks.Common;

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
           ,[CreatedAt]
           ,[BlockExecutionStatus]
           ,[Attempt])
     VALUES
           (@TaskExecutionId
           ,@BlockId
           ,GETUTCDATE()
           ,@Status
           ,@Attempt);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        public const string GetLastDateRangeBlockQuery = @"
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
AND IsPhantom = 0";

        public const string GetLastNumericRangeBlockQuery = @"
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
AND IsPhantom = 0";

        public static string GetLastDateRangeBlock(LastBlockOrder lastBlockOrder)
        {
            switch (lastBlockOrder)
            {
                case LastBlockOrder.LastCreated:
                    return GetLastDateRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
                case LastBlockOrder.MaxRangeStartValue:
                    return GetLastDateRangeBlockQuery + " ORDER BY [FromDate] DESC";
                case LastBlockOrder.MaxRangeEndValue:
                    return GetLastDateRangeBlockQuery + " ORDER BY [ToDate] DESC";
                default:
                    return GetLastDateRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
            }
        }

        public static string GetLastNumericRangeBlock(LastBlockOrder lastBlockOrder)
        {
            switch (lastBlockOrder)
            {
                case LastBlockOrder.LastCreated:
                    return GetLastNumericRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
                case LastBlockOrder.MaxRangeStartValue:
                    return GetLastNumericRangeBlockQuery + " ORDER BY [FromNumber] DESC";
                case LastBlockOrder.MaxRangeEndValue:
                    return GetLastNumericRangeBlockQuery + " ORDER BY [ToNumber] DESC";
                default:
                    return GetLastNumericRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
            }
        }
    }
}
