using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class BlockExecutionQueryBuilder
    {
        private const string UpdateBlockExecutionStatus = @"UPDATE {0}.[{1}BlockExecution]
   SET [BlockExecutionStatus] = @BlockExecutionStatus
 WHERE {1}BlockExecutionId = @{1}BlockExecutionId ";

        private const string SetBlockExecutionAsCompleted = @"UPDATE {0}.[{1}BlockExecution]
   SET [CompletedAt] = GETUTCDATE()
      ,[BlockExecutionStatus] = @BlockExecutionStatus
 WHERE {1}BlockExecutionId = @{1}BlockExecutionId ";

        public static string GetUpdateDateRangeBlockExecutionStatusQuery(string tableSchema)
        {
            return string.Format(UpdateBlockExecutionStatus, tableSchema, "DateRange");
        }

        public static string GetUpdateNumericRangeBlockExecutionStatusQuery(string tableSchema)
        {
            return string.Format(UpdateBlockExecutionStatus, tableSchema, "NumericRange");
        }

        public static string GetUpdateListBlockExecutionStatusQuery(string tableSchema)
        {
            return string.Format(UpdateBlockExecutionStatus, tableSchema, "List");
        }

        public static string GetSetDateRangeBlockExecutionAsCompletedQuery(string tableSchema)
        {
            return string.Format(SetBlockExecutionAsCompleted, tableSchema, "DateRange");
        }

        public static string GetSetNumericRangeBlockExecutionAsCompletedQuery(string tableSchema)
        {
            return string.Format(SetBlockExecutionAsCompleted, tableSchema, "NumericRange");
        }

        public static string GetSetListBlockExecutionAsCompletedQuery(string tableSchema)
        {
            return string.Format(SetBlockExecutionAsCompleted, tableSchema, "List");
        }
    }
}
