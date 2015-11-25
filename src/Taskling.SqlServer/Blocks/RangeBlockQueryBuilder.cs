using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class RangeBlockQueryBuilder
    {
        private const string FindFailedBlocksQuery = @"SELECT DISTINCT RBE.[{2}RangeBlockId]
INTO #NonFailedExecutions
FROM {1}.[{2}RangeBlockExecution] RBE
JOIN {1}.[TaskExecution] TE ON RBE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskSecondaryId = @TaskSecondaryId
AND RBE.StartedAt >= @FailedTaskDateLimit
AND RBE.BlockExecutionStatus IN (0, 1, 2)

SELECT TOP {0} RB.[{2}RangeBlockId] AS RangeBlockId
      ,RB.[From{3}] AS FromValue
      ,RB.[To{3}] AS ToValue
FROM {1}.[{2}RangeBlock] RB
JOIN {1}.[{2}RangeBlockExecution] RBE ON RB.{2}RangeBlockId = RBE.{2}RangeBlockId
LEFT JOIN #NonFailedExecutions NFE ON RB.{2}RangeBlockId = NFE.{2}RangeBlockId
WHERE RB.TaskSecondaryId = @TaskSecondaryId
AND RBE.StartedAt >= @FailedTaskDateLimit
AND RBE.BlockExecutionStatus = 3
AND NFE.{2}RangeBlockId IS NULL -- Discard any that have pending, in progress
-- or successfully completed executions
ORDER BY RB.[From{3}] ASC";

        private const string FindDeadBlocksQuery = @"SELECT DISTINCT RBE.[{2}RangeBlockId]
INTO #PostPeriodExecution
FROM {1}.[{2}RangeBlockExecution] RBE
JOIN {1}.[TaskExecution] TE ON RBE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskSecondaryId = @TaskSecondaryId
AND RBE.StartedAt > @SearchPeriodEnd

SELECT TOP {0} RB.[{2}RangeBlockId] AS RangeBlockId
      ,RB.[From{3}] AS FromValue
      ,RB.[To{3}] AS ToValue
FROM {1}.[{2}RangeBlock] RB
JOIN {1}.[{2}RangeBlockExecution] RBE ON RB.{2}RangeBlockId = RBE.{2}RangeBlockId
LEFT JOIN #PostPeriodExecution PPE ON RB.{2}RangeBlockId = PPE.{2}RangeBlockId
WHERE RB.TaskSecondaryId = @TaskSecondaryId
AND RBE.StartedAt >= @SearchPeriodBegin
AND RBE.StartedAt <= @SearchPeriodEnd
AND RBE.BlockExecutionStatus IN (0,1)
AND PPE.{2}RangeBlockId IS NULL -- Discard any that have had subsequent executions
ORDER BY RB.[From{3}] ASC";

        private const string FindDeadBlocksWithKeepAliveQuery = @"SELECT DISTINCT RBE.[{2}RangeBlockId]
INTO #PostPeriodExecution
FROM {1}.[{2}RangeBlockExecution] RBE
JOIN {1}.[TaskExecution] TE ON RBE.TaskExecutionId = TE.TaskExecutionId
WHERE TE.TaskSecondaryId = @TaskSecondaryId
AND RBE.StartedAt > @LastKeepAliveLimit

SELECT TOP {0} RB.[{2}RangeBlockId] AS RangeBlockId
      ,RB.[From{3}] AS FromValue
      ,RB.[To{3}] AS ToValue
FROM {1}.[{2}RangeBlock] RB
JOIN {1}.[{2}RangeBlockExecution] RBE ON RB.{2}RangeBlockId = RBE.{2}RangeBlockId
LEFT JOIN {1}.[TaskExecution] TE ON RBE.TaskExecutionId = TE.TaskExecutionId
LEFT JOIN #PostPeriodExecution PPE ON RB.{2}RangeBlockId = PPE.{2}RangeBlockId
WHERE RB.TaskSecondaryId = @TaskSecondaryId
AND RBE.BlockExecutionStatus IN (0,1)
AND TE.LastKeepAlive < @LastKeepAliveLimit
AND PPE.{2}RangeBlockId IS NULL -- Discard any that have had subsequent executions
ORDER BY RB.[From{3}] ASC";

        private const string InsertDateRangeBlock = @"INSERT INTO {0}.[DateRangeBlock]
           ([TaskSecondaryId]
           ,[FromDate]
           ,[ToDate])
     VALUES
           (@TaskSecondaryId
           ,@FromDate
           ,@ToDate);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertNumericRangeBlock = @"INSERT INTO {0}.[NumericRangeBlock]
           ([TaskSecondaryId]
           ,[FromNumber]
           ,[ToNumber])
     VALUES
           (@TaskSecondaryId
           ,@FromNumber
           ,@ToNumber);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertDateRangeBlockExecution = @"INSERT INTO {0}.[DateRangeBlockExecution]
           ([TaskExecutionId]
           ,[DateRangeBlockId]
           ,[StartedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@DateRangeBlockId
           ,GETUTCDATE()
           ,0);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string InsertNumericRangeBlockExecution = @"INSERT INTO {0}.[NumericRangeBlockExecution]
           ([TaskExecutionId]
           ,[NumericRangeBlockId]
           ,[StartedAt]
           ,[BlockExecutionStatus])
     VALUES
           (@TaskExecutionId
           ,@NumericRangeBlockId
           ,GETUTCDATE()
           ,0);

SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

        private const string UpdateBlockExecutionStatus = @"UPDATE {0}.[{1}RangeBlockExecution]
   SET [BlockExecutionStatus] = @BlockExecutionStatus
 WHERE {1}RangeBlockExecutionId = @{1}RangeBlockExecutionId ";

        private const string SetBlockExecutionAsCompleted = @"UPDATE {0}.[{1}RangeBlockExecution]
   SET [CompletedAt] = GETUTCDATE()
      ,[BlockExecutionStatus] = @BlockExecutionStatus
 WHERE {1}RangeBlockExecutionId = @{1}RangeBlockExecutionId ";

        public static string GetFindFailedDateRangeBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindFailedBlocksQuery, top, tableSchema, "Date", "Date");
        }

        public static string GetFindFailedNumericRangeBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindFailedBlocksQuery, top, tableSchema, "Numeric", "Number");
        }

        public static string GetFindDeadDateRangeBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindDeadBlocksQuery, top, tableSchema, "Date", "Date");
        }

        public static string GetFindDeadNumericRangeBlocksQuery(int top, string tableSchema)
        {
            return string.Format(FindDeadBlocksQuery, top, tableSchema, "Numeric", "Number");
        }

        public static string GetFindDeadDateRangeBlocksWithKeepAliveQuery(int top, string tableSchema)
        {
            return string.Format(FindDeadBlocksWithKeepAliveQuery, top, tableSchema, "Date", "Date");
        }

        public static string GetFindDeadNumericRangeBlocksWithKeepAliveQuery(int top, string tableSchema)
        {
            return string.Format(FindDeadBlocksWithKeepAliveQuery, top, tableSchema, "Numeric", "Number");
        }

        public static string GetInsertDateRangeBlockQuery(string tableSchema)
        {
            return string.Format(InsertDateRangeBlock, tableSchema);
        }

        public static string GetInsertDateRangeBlockExecutionQuery(string tableSchema)
        {
            return string.Format(InsertDateRangeBlockExecution, tableSchema);
        }

        public static string GetInsertNumericRangeBlockQuery(string tableSchema)
        {
            return string.Format(InsertNumericRangeBlock, tableSchema);
        }

        public static string GetInsertNumericRangeBlockExecutionQuery(string tableSchema)
        {
            return string.Format(InsertNumericRangeBlockExecution, tableSchema);
        }

        public static string GetUpdateDateRangeBlockExecutionStatusQuery(string tableSchema)
        {
            return string.Format(UpdateBlockExecutionStatus, tableSchema, "Date");
        }

        public static string GetUpdateNumericRangeBlockExecutionStatusQuery(string tableSchema)
        {
            return string.Format(UpdateBlockExecutionStatus, tableSchema, "Numeric");
        }

        public static string GetSetDateRangeBlockExecutionAsCompletedQuery(string tableSchema)
        {
            return string.Format(SetBlockExecutionAsCompleted, tableSchema, "Date");
        }

        public static string GetSetNumericRangeBlockExecutionAsCompletedQuery(string tableSchema)
        {
            return string.Format(SetBlockExecutionAsCompleted, tableSchema, "Numeric");
        }
    }
}
