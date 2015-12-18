using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution
{
    internal class TaskQueryBuilder
    {
        #region .: Tasks :.

        public const string GetTaskQuery = @"SELECT [ApplicationName]
      ,[TaskName]
      ,[TaskDefinitionId]
FROM [Taskling].[TaskDefinition]
WHERE [ApplicationName] = @ApplicationName
AND [TaskName] = @TaskName";

        public const string InsertTaskQuery = @"INSERT INTO [Taskling].[TaskDefinition]([ApplicationName],[TaskName])
VALUES(@ApplicationName,@TaskName)

SELECT TaskDefinitionId
FROM [Taskling].[TaskDefinition]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Tasks :.

        #region .: TaskExecutions :.

        public const string InsertKeepAliveTaskExecution = @"
INSERT INTO [Taskling].[TaskExecution]([TaskDefinitionId],[StartedAt],[ServerName],[LastKeepAlive],[TaskDeathMode],[KeepAliveInterval],[KeepAliveDeathThreshold])
VALUES (@TaskDefinitionId, GETUTCDATE(), @ServerName, GETUTCDATE(), @TaskDeathMode, @KeepAliveInterval, @KeepAliveDeathThreshold);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

//            public const string InsertKeepAlive = @"
//INSERT INTO [TasklingDb].[Taskling].[KeepAlive]([TaskDefinitionId],[TaskExecutionId],[LastKeepAlive])
//VALUES(@TaskDefinitionId, @TaskExecutionId, GETUTCDATE())";

        public const string InsertOverrideTaskExecution = @"
INSERT INTO [Taskling].[TaskExecution]([TaskDefinitionId],[StartedAt],[ServerName],[LastKeepAlive],[TaskDeathMode],[OverrideThreshold])
VALUES (@TaskDefinitionId, GETUTCDATE(), @ServerName, GETUTCDATE(), @TaskDeathMode, @OverrideThreshold);
SELECT CAST(SCOPE_IDENTITY() AS INT)";

        public const string KeepAliveQuery = @"
UPDATE ET
SET [LastKeepAlive] = GETUTCDATE()
FROM [Taskling].[ExecutionToken] ET
WHERE [TaskDefinitionId] = @TaskDefinitionId
AND [ExecutionTokenId] = @ExecutionTokenId;

UPDATE TE
SET [LastKeepAlive] = GETUTCDATE()
FROM [Taskling].[TaskExecution] TE
WHERE [TaskExecutionId] = @TaskExecutionId;";

        #endregion .: TaskExecutions :.
    }
}
