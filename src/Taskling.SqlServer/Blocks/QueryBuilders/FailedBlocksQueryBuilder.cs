using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class FailedBlocksQueryBuilder
    {
        private const string FindFailedBlocksQuery = @"SELECT DISTINCT BE.[{2}BlockId]
INTO #NonFailedExecutions
FROM {1}.[{2}BlockExecution] BE
JOIN {1}.[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskSecondaryId = @TaskSecondaryId
AND BE.StartedAt >= @FailedTaskDateLimit
AND BE.BlockExecutionStatus IN (0, 1, 2)

SELECT TOP {0} B.[{2}BlockId] AS BlockId
      {3}
FROM {1}.[{2}Block] B
JOIN {1}.[{2}BlockExecution] BE ON B.{2}BlockId = BE.{2}BlockId
LEFT JOIN #NonFailedExecutions NFE ON B.{2}BlockId = NFE.{2}BlockId
WHERE B.TaskSecondaryId = @TaskSecondaryId
AND BE.StartedAt >= @FailedTaskDateLimit
AND BE.BlockExecutionStatus = 3
AND NFE.{2}BlockId IS NULL -- Discard any that have pending, in progress
-- or successfully completed executions
ORDER BY B.[{4}] ASC";

        public static string GetFindFailedDateRangeBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindFailedBlocksQuery, top, tableSchema, "DateRange", ",B.FromDate,B.ToDate", "FromDate");
        }

        public static string GetFindFailedNumericRangeBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindFailedBlocksQuery, top, tableSchema, "NumericRange", ",B.FromNumber,B.ToNumber", "FromNumber");
        }

        public static string GetFindFailedListBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindFailedBlocksQuery, top, tableSchema, "List", "", "CreatedDate");
        }
    }
}
