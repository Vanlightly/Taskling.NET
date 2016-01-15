using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class DeadBlocksQueryBuilder
    {
        private const string FindDeadBlocksQuery = @"SELECT DISTINCT BE.[BlockId]
INTO #PostPeriodExecution
FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskDefinitionId = @TaskDefinitionId
AND BE.StartedAt > @SearchPeriodEnd

SELECT TOP {0} B.[BlockId]
      {1}
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
LEFT JOIN #PostPeriodExecution PPE ON B.BlockId = PPE.BlockId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND BE.StartedAt >= @SearchPeriodBegin
AND BE.StartedAt <= @SearchPeriodEnd
AND BE.BlockExecutionStatus IN (0,1)
AND PPE.BlockId IS NULL -- Discard any that have had subsequent executions
ORDER BY B.CreatedDate ASC";

        private const string FindDeadBlocksWithKeepAliveQuery = @"SELECT DISTINCT BE.[BlockId]
INTO #PostPeriodExecution
FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskDefinitionId = @TaskDefinitionId
AND BE.StartedAt > @LastKeepAliveLimit

SELECT TOP {0} B.[BlockId] 
      {1}
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
LEFT JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN #PostPeriodExecution PPE ON B.BlockId = PPE.BlockId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND BE.BlockExecutionStatus IN (0,1)
AND TE.LastKeepAlive < @LastKeepAliveLimit
AND PPE.BlockId IS NULL -- Discard any that have had subsequent executions
ORDER BY B.CreatedDate ASC";

        public static string GetFindDeadDateRangeBlocksQuery(int top)
        {
            return String.Format(FindDeadBlocksQuery, top, ",B.FromDate,B.ToDate");
        }

        public static string GetFindDeadNumericRangeBlocksQuery(int top)
        {
            return String.Format(FindDeadBlocksQuery, top, ",B.FromNumber,B.ToNumber");
        }

        public static string GetFindDeadListBlocksQuery(int top)
        {
            return String.Format(FindDeadBlocksQuery, top, "");
        }

        public static string GetFindDeadDateRangeBlocksWithKeepAliveQuery(int top)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, ",B.FromDate,B.ToDate");
        }

        public static string GetFindDeadNumericRangeBlocksWithKeepAliveQuery(int top)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, ",B.FromNumber,B.ToNumber");
        }

        public static string GetFindDeadListBlocksWithKeepAliveQuery(int top)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, "");
        }
    }
}
