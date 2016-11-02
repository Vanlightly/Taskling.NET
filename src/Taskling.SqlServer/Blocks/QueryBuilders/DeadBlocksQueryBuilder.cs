using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class DeadBlocksQueryBuilder
    {
        private const string FindDeadBlocksQuery = @"WITH OrderedBlocks As (
	SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo
			,BE.[BlockExecutionId]
	FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
	JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
	WHERE TE.TaskDefinitionId = @TaskDefinitionId
	AND TE.StartedAt >= @SearchPeriodBegin
    AND TE.StartedAt < @SearchPeriodEnd
)

SELECT TOP {0} B.[BlockId]
    {1}
    ,BE.Attempt
    ,B.BlockType
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND TE.StartedAt <= DATEADD(SECOND, -1 * DATEDIFF(SECOND, '00:00:00', OverrideThreshold), GETUTCDATE())
AND BE.BlockExecutionStatus IN (1,2)
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
ORDER BY B.CreatedDate ASC";

        private const string FindDeadBlocksWithKeepAliveQuery = @"WITH OrderedBlocks As (
	SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo
			,BE.[BlockExecutionId]
	FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
	JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
	WHERE TE.TaskDefinitionId = @TaskDefinitionId
	AND TE.StartedAt  >= @SearchPeriodBegin
    AND TE.StartedAt < @SearchPeriodEnd
)

SELECT TOP {0} B.[BlockId]
    {1}
    ,BE.Attempt
    ,B.BlockType
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND DATEDIFF(SECOND, TE.LastKeepAlive, GETUTCDATE()) > DATEDIFF(SECOND, '00:00:00', TE.KeepAliveDeathThreshold)
AND BE.BlockExecutionStatus IN (1,2)
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
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

        public static string GetFindDeadObjectBlocksQuery(int top)
        {
            return String.Format(FindDeadBlocksQuery, top, ",B.ObjectData");
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

        public static string GetFindDeadObjectBlocksWithKeepAliveQuery(int top)
        {
            return String.Format(FindDeadBlocksWithKeepAliveQuery, top, ",B.ObjectData");
        }
    }
}
