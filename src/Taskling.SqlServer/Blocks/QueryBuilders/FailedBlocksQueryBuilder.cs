using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class FailedBlocksQueryBuilder
    {
        private const string FindFailedBlocksQuery = @"SELECT DISTINCT BE.[BlockId]
INTO #NonFailedExecutions
FROM [Taskling].[BlockExecution] BE
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskDefinitionId = @TaskDefinitionId
AND BE.StartedAt >= @FailedTaskDateLimit
AND BE.BlockExecutionStatus IN (0, 1, 2)

SELECT TOP {0} B.[BlockId] AS BlockId
      {1}
FROM [Taskling].[Block] B
JOIN [Taskling].[BlockExecution] BE ON B.BlockId = BE.BlockId
LEFT JOIN #NonFailedExecutions NFE ON B.BlockId = NFE.BlockId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND BE.StartedAt >= @FailedTaskDateLimit
AND BE.BlockExecutionStatus = 3
AND NFE.BlockId IS NULL -- Discard any that have pending, in progress
-- or successfully completed executions
ORDER BY B.CreatedDate ASC";

        public static string GetFindFailedDateRangeBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, ",B.FromDate,B.ToDate");
        }

        public static string GetFindFailedNumericRangeBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, ",B.FromNumber,B.ToNumber");
        }

        public static string GetFindFailedListBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, "");
        }
    }
}
