using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution.QueryBuilders
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

        public const string GetLastCleanUpTimeQuery = @"SELECT [LastCleaned]
FROM [Taskling].[TaskDefinition] WITH(NOLOCK)
WHERE [ApplicationName] = @ApplicationName
AND [TaskName] = @TaskName";

        public const string SetLastCleanUpTimeQuery = @"UPDATE [Taskling].[TaskDefinition]
SET [LastCleaned] = GETUTCDATE()
WHERE [ApplicationName] = @ApplicationName
AND [TaskName] = @TaskName";

        public const string InsertTaskQuery = @"INSERT INTO [Taskling].[TaskDefinition]([ApplicationName],[TaskName],[LastCleaned],[UserCsStatus],[ClientCsStatus])
VALUES(@ApplicationName,@TaskName,GETUTCDATE(),1,1)

SELECT TaskDefinitionId
FROM [Taskling].[TaskDefinition]
WHERE ApplicationName = @ApplicationName
AND TaskName = @TaskName";

        #endregion .: Tasks :.

        #region .: TaskExecutions :.

        public const string InsertKeepAliveTaskExecution = @"
INSERT INTO [Taskling].[TaskExecution]([TaskDefinitionId],[StartedAt],[ServerName],[LastKeepAlive],[TaskDeathMode],[KeepAliveInterval],[KeepAliveDeathThreshold],
        [FailedTaskRetryLimit],[DeadTaskRetryLimit],[ReferenceValue],[Failed],[Blocked],[TasklingVersion],[ExecutionHeader])
VALUES (@TaskDefinitionId, GETUTCDATE(), @ServerName, GETUTCDATE(), @TaskDeathMode, @KeepAliveInterval, @KeepAliveDeathThreshold, 
        @FailedTaskRetryLimit, @DeadTaskRetryLimit, @ReferenceValue, 0, 0,@TasklingVersion,@ExecutionHeader);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

        //            public const string InsertKeepAlive = @"
        //INSERT INTO [TasklingDb].[Taskling].[KeepAlive]([TaskDefinitionId],[TaskExecutionId],[LastKeepAlive])
        //VALUES(@TaskDefinitionId, @TaskExecutionId, GETUTCDATE())";

        public const string InsertOverrideTaskExecution = @"
INSERT INTO [Taskling].[TaskExecution]([TaskDefinitionId],[StartedAt],[ServerName],[LastKeepAlive],[TaskDeathMode],[OverrideThreshold],
            [FailedTaskRetryLimit],[DeadTaskRetryLimit],[ReferenceValue],[Failed],[Blocked],[TasklingVersion],[ExecutionHeader])
VALUES (@TaskDefinitionId, GETUTCDATE(), @ServerName, GETUTCDATE(), @TaskDeathMode, @OverrideThreshold, 
            @FailedTaskRetryLimit, @DeadTaskRetryLimit, @ReferenceValue, 0, 0,@TasklingVersion,@ExecutionHeader);
SELECT CAST(SCOPE_IDENTITY() AS INT)";

        public const string KeepAliveQuery = @"
UPDATE TE
SET [LastKeepAlive] = GETUTCDATE()
FROM [Taskling].[TaskExecution] TE
WHERE [TaskExecutionId] = @TaskExecutionId;";

        public const string SetCompletedDateOfTaskExecutionQuery = @"
UPDATE [Taskling].[TaskExecution]
SET [CompletedAt] = GETUTCDATE()
WHERE TaskExecutionId = @TaskExecutionId";

        public const string SetBlockedTaskExecutionQuery = @"
UPDATE [Taskling].[TaskExecution]
SET [Blocked] = 1
WHERE TaskExecutionId = @TaskExecutionId";

        public const string SetTaskExecutionAsFailedQuery = @"
UPDATE [Taskling].[TaskExecution]
SET [Failed] = 1
WHERE TaskExecutionId = @TaskExecutionId";

        public const string GetLastExecutionQuery = @"SELECT TOP(@Top) [TaskExecutionId]
      ,[TaskDefinitionId]
      ,[StartedAt]
      ,[CompletedAt]
      ,[LastKeepAlive]
      ,[ServerName]
      ,[TaskDeathMode]
      ,[OverrideThreshold]
      ,[KeepAliveInterval]
      ,[KeepAliveDeathThreshold]
      ,[FailedTaskRetryLimit]
      ,[DeadTaskRetryLimit]
      ,[ReferenceValue]
      ,[Failed]
      ,[Blocked]
      ,[TasklingVersion]
      ,[ExecutionHeader]
      ,GETUTCDATE() AS DbServerUtcNow
FROM [Taskling].[TaskExecution]
WHERE [TaskDefinitionId] = @TaskDefinitionId
ORDER BY [TaskExecutionId] DESC";

        #endregion .: TaskExecutions :.

        #region .: TaskExecutionEvents :.

        public const string InsertTaskExecutionEventQuery = @"
INSERT INTO [Taskling].[TaskExecutionEvent]
           ([TaskExecutionId]
           ,[EventType]
           ,[Message]
           ,[EventDateTime])
     VALUES
           (@TaskExecutionId
           ,@EventType
           ,@Message
           ,@EventDateTime)";

        #endregion .: TaskExecutionEvents :.
    }
}
