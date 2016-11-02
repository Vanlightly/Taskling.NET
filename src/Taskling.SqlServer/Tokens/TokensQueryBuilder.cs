using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.Tokens
{
    public class TokensQueryBuilder
    {
        #region .: Common :.

        public const string AcquireLockQuery = @"UPDATE Taskling.[TaskDefinition]
SET [HoldLockTaskExecutionId] = @TaskExecutionId
WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        public const string GetTaskExecutionsBaseQuery = @"SELECT [TaskExecutionId]
      ,[StartedAt]
      ,[CompletedAt]
      ,[LastKeepAlive]
      ,[TaskDeathMode]
      ,[OverrideThreshold]
      ,[KeepAliveInterval]
      ,[KeepAliveDeathThreshold]
      ,GETUTCDATE() AS [CurrentDateTime]
  FROM [Taskling].[TaskExecution]
  WHERE [TaskExecutionId] IN ";

        public static string GetTaskExecutions(int taskExecutionCount)
        {
            var sb = new StringBuilder();
            sb.Append(GetTaskExecutionsBaseQuery);

            sb.Append("(");
            for (int i = 0; i < taskExecutionCount; i++)
            {
                if (i > 0)
                    sb.Append(",");

                sb.Append("@InParam" + i);
            }

            sb.Append(")");

            return sb.ToString();
        }

        #endregion .: Common :.

        #region .: Critical Sections :.

        public const string GetUserCriticalSectionStateQuery = @"SELECT [UserCsStatus]
      ,[UserCsTaskExecutionId]
      ,[UserCsQueue]
FROM [Taskling].[TaskDefinition]
WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        public const string GetClientCriticalSectionStateQuery = @"SELECT [ClientCsStatus]
      ,[ClientCsTaskExecutionId]
      ,[ClientCsQueue]
FROM [Taskling].[TaskDefinition]
WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        public const string SetUserCriticalSectionStateQuery = @"UPDATE [Taskling].[TaskDefinition]
SET [UserCsStatus] = @CsStatus
      ,[UserCsTaskExecutionId] = @CsTaskExecutionId
      ,[UserCsQueue] = @CsQueue
WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        public const string SetClientCriticalSectionStateQuery = @"UPDATE [Taskling].[TaskDefinition]
SET [ClientCsStatus] = @CsStatus
      ,[ClientCsTaskExecutionId] = @CsTaskExecutionId
      ,[ClientCsQueue] = @CsQueue
WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        public const string ReturnUserCriticalSectionTokenQuery = @"UPDATE [Taskling].[TaskDefinition] 
	SET [UserCsStatus] = 1
	WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        public const string ReturnClientCriticalSectionTokenQuery = @"UPDATE [Taskling].[TaskDefinition] 
	SET [ClientCsStatus] = 1
	WHERE [TaskDefinitionId] = @TaskDefinitionId;";

        #endregion .: Critical Sections :.

        #region .: Executions :.

        public const string GetExecutionTokensQuery = @"SELECT [ExecutionTokens]
FROM [Taskling].[TaskDefinition]
WHERE TaskDefinitionId = @TaskDefinitionId";

        public const string UpdateExecutionTokensQuery = @"UPDATE [Taskling].[TaskDefinition]
SET [ExecutionTokens] = @ExecutionTokens
WHERE TaskDefinitionId = @TaskDefinitionId";

        #endregion .: Executions :.







    }
}
