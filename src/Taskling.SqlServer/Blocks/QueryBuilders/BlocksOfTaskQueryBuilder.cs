using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Taskling.Tasks;

namespace Taskling.SqlServer.Blocks.QueryBuilders
{
    public class BlocksOfTaskQueryBuilder
    {
        private const string GetBlocksOfTaskQuery = @"
SELECT B.[BlockId]
        {0}
        ,BE.Attempt
        ,B.BlockType
        ,B.ObjectData
        ,B.CompressedObjectData
FROM [Taskling].[Block] B WITH(NOLOCK)
JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId
LEFT JOIN [Taskling].[TaskExecution] TE WITH(NOLOCK) ON BE.TaskExecutionId = TE.TaskExecutionId
WHERE B.TaskDefinitionId = @TaskDefinitionId
AND TE.ReferenceValue = @ReferenceValue
{1}
ORDER BY B.CreatedDate ASC";

        public static string GetFindDateRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
        {
            if (reprocessOption == ReprocessOption.Everything)
                return String.Format(GetBlocksOfTaskQuery, ",B.FromDate,B.ToDate", "");

            if (reprocessOption == ReprocessOption.PendingOrFailed)
                return String.Format(GetBlocksOfTaskQuery, ",B.FromDate,B.ToDate", "AND BE.BlockExecutionStatus IN (0, 1, 3)");

            throw new ArgumentException("ReprocessOption not supported");
        }

        public static string GetFindNumericRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
        {
            if (reprocessOption == ReprocessOption.Everything)
                return String.Format(GetBlocksOfTaskQuery, ",B.FromNumber,B.ToNumber", "");

            if (reprocessOption == ReprocessOption.PendingOrFailed)
                return String.Format(GetBlocksOfTaskQuery, ",B.FromNumber,B.ToNumber", "AND BE.BlockExecutionStatus IN (0, 1, 3)");

            throw new ArgumentException("ReprocessOption not supported");
        }

        public static string GetFindListBlocksOfTaskQuery(ReprocessOption reprocessOption)
        {
            if (reprocessOption == ReprocessOption.Everything)
                return String.Format(GetBlocksOfTaskQuery, "", "");

            if (reprocessOption == ReprocessOption.PendingOrFailed)
                return String.Format(GetBlocksOfTaskQuery, "", "AND BE.BlockExecutionStatus IN (@NotStarted, @Started, @Failed)");

            throw new ArgumentException("ReprocessOption not supported");
        }

        public static string GetFindObjectBlocksOfTaskQuery(ReprocessOption reprocessOption)
        {
            if (reprocessOption == ReprocessOption.Everything)
                return String.Format(GetBlocksOfTaskQuery, ",B.ObjectData", "");

            if (reprocessOption == ReprocessOption.PendingOrFailed)
                return String.Format(GetBlocksOfTaskQuery, ",B.ObjectData", "AND BE.BlockExecutionStatus IN (@NotStarted, @Started, @Failed)");

            throw new ArgumentException("ReprocessOption not supported");
        }
    }
}
