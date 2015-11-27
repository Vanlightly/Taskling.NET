using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Blocks
{
    internal class RangeBlockQueryBuilder
    {
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

        public static string GetInsertDateRangeBlockQuery(string tableSchema)
        {
            return String.Format(InsertDateRangeBlock, tableSchema);
        }

        public static string GetInsertDateRangeBlockExecutionQuery(string tableSchema)
        {
            return String.Format(InsertDateRangeBlockExecution, tableSchema);
        }

        public static string GetInsertNumericRangeBlockQuery(string tableSchema)
        {
            return String.Format(InsertNumericRangeBlock, tableSchema);
        }

        public static string GetInsertNumericRangeBlockExecutionQuery(string tableSchema)
        {
            return String.Format(InsertNumericRangeBlockExecution, tableSchema);
        }
    }
}
