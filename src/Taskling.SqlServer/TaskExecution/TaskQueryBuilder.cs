using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution
{
    internal class TaskQueryBuilder
    {
        #region .: Tasks :.

        private const string GetTaskQueryBase = @"SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskSecondaryId]
FROM {0}.[Task]
WHERE [ApplicationName] = @ApplicationName
AND [TaskName] = @TaskName";

        internal static string GetTaskQuery(string schema)
        {
            return string.Format(GetTaskQueryBase, schema);
        }

        private const string InsertTaskQueryBase = @"INSERT INTO {0}.[Task]([ApplicationName],[TaskName])
VALUES(@ApplicationName,@TaskName)

SELECT TaskSecondaryId
FROM {0}.[Task]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        internal static string InsertTaskQuery(string schema)
        {
            return string.Format(InsertTaskQueryBase, schema);
        }

        #endregion .: Tasks :.

        #region .: TaskExecutions :.

        private const string InsertTaskExecutionBase = @"INSERT INTO {0}.[TaskExecution]([TaskSecondaryId],[StartedAt])
VALUES (@TaskSecondaryId, GETDATE());
SELECT CAST(SCOPE_IDENTITY() AS INT)";

        internal static string InsertTaskExecution(string schema)
        {
            return string.Format(InsertTaskExecutionBase, schema);
        }

        private const string KeepAliveQueryBase = @"UPDATE {0}.[ExecutionTokens]
SET LastKeepAlive = GETDATE()
WHERE [TaskExecutionId] = @TaskExecutionId";

        internal static string KeepAliveQuery(string schema)
        {
            return string.Format(KeepAliveQueryBase, schema);
        }

        #endregion .: TaskExecutions :.
    }
}
