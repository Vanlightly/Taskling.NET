using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution.QueryBuilders
{
    internal class CleanUpQueryBuilder
    {
        public const string IdentifyOldBlocksQuery = @"
SELECT [BlockId]
FROM [Taskling].[Block] WITH(NOLOCK)
WHERE [TaskDefinitionId] = @TaskDefinitionId
AND [CreatedDate] < @OlderThanDate";

        public const string DeleteListItemsOfBlockQuery = @"
DELETE FROM [Taskling].[ListBlockItem]
WHERE BlockId = @BlockId";

        public const string DeleteOldDataQuery = @"
DELETE FROM [Taskling].[BlockExecution]
WHERE BlockExecutionId IN (
    SELECT BlockExecutionId
    FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)
    LEFT JOIN [Taskling].[Block] B WITH(NOLOCK) ON BE.BlockId = B.BlockId
    WHERE (B.TaskDefinitionId = @TaskDefinitionId
    AND B.CreatedDate < @OlderThanDate)
    OR B.TaskDefinitionId IS NULL
);

DELETE FROM [Taskling].[Block]
WHERE BlockId IN (
    SELECT BlockId
    FROM [Taskling].[Block] WITH(NOLOCK)
    WHERE TaskDefinitionId = @TaskDefinitionId
    AND CreatedDate < @OlderThanDate
);

DELETE FROM [Taskling].[TaskExecutionEvent]
WHERE TaskExecutionEventId IN (
    SELECT TaskExecutionEventId
    FROM [Taskling].[TaskExecutionEvent] TEE WITH(NOLOCK) 
    LEFT JOIN [Taskling].[TaskExecution] TE WITH(NOLOCK) ON TEE.TaskExecutionId  = TE.TaskExecutionId  
    WHERE (TE.TaskDefinitionId = @TaskDefinitionId
    AND TE.StartedAt < @OlderThanDate)
    OR TE.TaskDefinitionId IS NULL
);

DELETE FROM [Taskling].[TaskExecution]
WHERE TaskExecutionId IN (
    SELECT TaskExecutionId
    FROM [Taskling].[TaskExecution] WITH(NOLOCK)
    WHERE TaskDefinitionId = @TaskDefinitionId
    AND StartedAt < @OlderThanDate
);

DELETE FROM [Taskling].[ForceBlockQueue]
WHERE ForcedDate < @OlderThanDate;";
    }
}
