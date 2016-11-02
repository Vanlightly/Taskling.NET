using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks.QueryBuilders
{
    public class ForcedBlockQueueQueryBuilder
    {
        private const string GetForcedBlocksQuery = @"SELECT B.[BlockId]
    {0}
    ,COALESCE(MaxAttempt, 0) AS Attempt
    ,B.BlockType
    ,FBQ.ForceBlockQueueId
    ,B.ObjectData
    ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[ForceBlockQueue] FBQ ON B.BlockId = FBQ.BlockId
OUTER APPLY (
	SELECT MAX(Attempt) MaxAttempt
	FROM [Taskling].[BlockExecution] WITH(NOLOCK) WHERE BlockId = FBQ.BlockId
) _
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND FBQ.ProcessingStatus = 'Pending'";

        public static string GetDateRangeBlocksQuery()
        {
            return string.Format(GetForcedBlocksQuery, ",B.FromDate,B.ToDate");
        }

        public static string GetNumericRangeBlocksQuery()
        {
            return string.Format(GetForcedBlocksQuery, ",B.FromNumber,B.ToNumber");
        }

        public static string GetListBlocksQuery()
        {
            return string.Format(GetForcedBlocksQuery, "");
        }

        public static string GetObjectBlocksQuery()
        {
            return string.Format(GetForcedBlocksQuery, ",B.ObjectData");
        }

        private const string UpdateQuery = @"UPDATE [Taskling].[ForceBlockQueue]
SET [ProcessingStatus] = 'Execution Created'
WHERE ForceBlockQueueId IN ({0})";

        public static string GetUpdateQuery(int blockCount)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < blockCount; i++)
            {
                if (i > 0)
                    sb.Append(",");

                sb.Append("@P" + i);
            }

            return string.Format(UpdateQuery, sb.ToString());
        }
    }
}
