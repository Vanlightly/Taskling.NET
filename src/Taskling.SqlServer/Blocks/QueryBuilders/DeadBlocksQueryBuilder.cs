using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class DeadBlocksQueryBuilder
    {
        private const string FindDeadBlocksQuery = @"SELECT DISTINCT BE.[{2}BlockId]
INTO #PostPeriodExecution
FROM {1}.[{2}BlockExecution] BE
JOIN {1}.[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskSecondaryId = @TaskSecondaryId
AND BE.StartedAt > @SearchPeriodEnd

SELECT TOP {0} B.[{2}BlockId] AS BlockId
      {3}
FROM {1}.[{2}Block] B
JOIN {1}.[{2}BlockExecution] BE ON B.{2}BlockId = BE.{2}BlockId
LEFT JOIN #PostPeriodExecution PPE ON B.{2}BlockId = PPE.{2}BlockId
WHERE B.TaskSecondaryId = @TaskSecondaryId
AND BE.StartedAt >= @SearchPeriodBegin
AND BE.StartedAt <= @SearchPeriodEnd
AND BE.BlockExecutionStatus IN (0,1)
AND PPE.{2}BlockId IS NULL -- Discard any that have had subsequent executions
ORDER BY B.CreatedDate ASC";

        private const string FindDeadBlocksWithKeepAliveQuery = @"SELECT DISTINCT BE.[{2}BlockId]
INTO #PostPeriodExecution
FROM {1}.[{2}BlockExecution] BE
JOIN {1}.[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskSecondaryId = @TaskSecondaryId
AND BE.StartedAt > @LastKeepAliveLimit

SELECT TOP {0} B.[{2}BlockId] AS BlockId
      {3}
FROM {1}.[{2}Block] B
JOIN {1}.[{2}BlockExecution] BE ON B.{2}BlockId = BE.{2}BlockId
LEFT JOIN {1}.[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN #PostPeriodExecution PPE ON B.{2}BlockId = PPE.{2}BlockId
WHERE B.TaskSecondaryId = @TaskSecondaryId
AND BE.BlockExecutionStatus IN (0,1)
AND TE.LastKeepAlive < @LastKeepAliveLimit
AND PPE.{2}BlockId IS NULL -- Discard any that have had subsequent executions
ORDER BY B.CreatedDate ASC";

        public static string GetFindDeadDateRangeBlocksQuery(int top, string tableSchema)
        {
            return String.Format(FindDeadBlocksQuery, top, tableSchema, "DateRange", ",B.FromDate,B.ToDate");
        }

        public static string GetFindDeadNumericRangeBlocksQuery(int top, string tableSchema)
        {
            return String.Format(FindDeadBlocksQuery, top, tableSchema, "NumericRange", ",B.FromNumber,B.ToNumber");
        }

        public static string GetFindDeadListBlocksQuery(int top, string tableSchema)
        {
            return String.Format(FindDeadBlocksQuery, top, tableSchema, "List", "");
        }

        public static string GetFindDeadDateRangeBlocksWithKeepAliveQuery(int top, string tableSchema)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, tableSchema, "DateRange", ",B.FromDate,B.ToDate");
        }

        public static string GetFindDeadNumericRangeBlocksWithKeepAliveQuery(int top, string tableSchema)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, tableSchema, "NumericRange", ",B.FromNumber,B.ToNumber");
        }

        public static string GetFindDeadListBlocksWithKeepAliveQuery(int top, string tableSchema)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, tableSchema, "List", "");
        }
    }
}
