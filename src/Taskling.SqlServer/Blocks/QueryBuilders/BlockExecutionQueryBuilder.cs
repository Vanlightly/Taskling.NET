using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class BlockExecutionQueryBuilder
    {
        public const string UpdateBlockExecutionStatus = @"UPDATE [Taskling].[BlockExecution]
   SET [BlockExecutionStatus] = @BlockExecutionStatus
 WHERE BlockExecutionId = @BlockExecutionId ";

        public const string SetBlockExecutionAsCompleted = @"UPDATE [Taskling].[BlockExecution]
   SET [CompletedAt] = GETUTCDATE()
      ,[BlockExecutionStatus] = @BlockExecutionStatus
 WHERE BlockExecutionId = @BlockExecutionId ";
    }
}
