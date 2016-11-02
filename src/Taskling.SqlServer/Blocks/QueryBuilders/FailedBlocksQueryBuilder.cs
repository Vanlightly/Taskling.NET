using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class FailedBlocksQueryBuilder
    {
        private const string FindFailedBlocksQuery = @"
WITH OrderedBlocks As (
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
    ,TE.ReferenceValue
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId
JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND B.IsPhantom = 0
AND BE.BlockExecutionStatus = 4
AND BE.Attempt < @AttemptLimit
AND OB.RowNo = 1
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

        public static string GetFindFailedObjectBlocksQuery(int top)
        {
            return string.Format(FindFailedBlocksQuery, top, ",B.ObjectData");
        }
    }
}
