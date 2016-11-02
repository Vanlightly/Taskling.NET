using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class BlockExecutionQueryBuilder
    {
        public const string SetBlockExecutionStatusToStarted = @"UPDATE [Taskling].[BlockExecution]
   SET [BlockExecutionStatus] = @BlockExecutionStatus
        ,[StartedAt] = GETUTCDATE()
 WHERE BlockExecutionId = @BlockExecutionId ";

        public const string SetRangeBlockExecutionAsCompleted = @"UPDATE [Taskling].[BlockExecution]
   SET [CompletedAt] = GETUTCDATE()
      ,[BlockExecutionStatus] = @BlockExecutionStatus
      ,[ItemsCount] = @ItemsCount
 WHERE BlockExecutionId = @BlockExecutionId ";

        public const string SetListBlockExecutionAsCompleted = @"UPDATE [Taskling].[BlockExecution]
   SET [CompletedAt] = GETUTCDATE()
      ,[BlockExecutionStatus] = @BlockExecutionStatus
 WHERE BlockExecutionId = @BlockExecutionId ";
    }
}
