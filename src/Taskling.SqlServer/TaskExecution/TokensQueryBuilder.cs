using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution
{
    internal class TokensQueryBuilder
    {
        #region .: Executions :.

        public const string OverrideBasedRequestExecutionTokenQuery = @"
    ----------------------------
-- Get an exclusive lock on the records of this process
----------------------------
UPDATE [PC].[ExecutionTokens]
SET [HoldLockTaskExecutionId] = @TaskExecutionId
WHERE [TaskSecondaryId] = @TaskSecondaryId;

----------------------------
-- Get 
-- an available token into a temporary table
-- or an unavailable token which can be overridden
-- but take available tokens before unavailable
----------------------------
DECLARE @AvailableToken TABLE  (
	[ExecutionTokenId] UniqueIdentifier NOT NULL
	,[LastGranteeId] int NULL
	,[TaskSecondaryId] int NOT NULL
	,[DateGranted] datetime NULL
	,[DateReturned] datetime NULL
	,[Status] tinyint NOT NULL
)

INSERT INTO @AvailableToken([ExecutionTokenId],[TaskSecondaryId],[DateGranted],[DateReturned],[Status])
SELECT TOP 1 [ExecutionTokenId]
	  ,[TaskSecondaryId]
	  ,[DateGranted]
	  ,[DateReturned]
	  ,[Status]
FROM [PC].[ExecutionTokens]
WHERE [TaskSecondaryId] = @TaskSecondaryId 
AND ([Status] = 1
	OR
	(DATEDIFF(SECOND, [DateGranted], GETDATE()) > @SecondsOverride
	AND [Status] = 0))
ORDER BY [Status] DESC;

----------------------------
-- If a viable token exists then update it as unavailable and return it
----------------------------
IF EXISTS(SELECT 1 FROM @AvailableToken)
BEGIN

	UPDATE [PC].[ExecutionTokens]
	SET [DateGranted] = GETDATE()
		,[DateReturned] = NULL
		,[Status] = 0
		,[TaskExecutionId] = @TaskExecutionId
	WHERE [ExecutionTokenId] = (SELECT TOP 1 [ExecutionTokenId] FROM @AvailableToken);

	SELECT TOP 1 [ExecutionTokenId]
            ,GETDATE() AS [StartedAt]
			,1 AS [Status]
	FROM @AvailableToken;

END
ELSE
BEGIN

	SELECT '00000000-0000-0000-0000-000000000000' AS [ExecutionTokenId]
            ,GETDATE() AS [StartedAt]
			,0 AS [Status];

END
";

        internal const string KeepAliveBasedRequestExecutionTokenQuery = @"----------------------------
-- Get an exclusive lock on the records of this process
----------------------------
UPDATE [PC].[ExecutionTokens]
SET [HoldLockTaskExecutionId] = @TaskExecutionId
WHERE [TaskSecondaryId] = @TaskSecondaryId;

----------------------------
-- Get 
-- an available token into a temporary table
-- or an unavailable token which can be overridden
-- but take available tokens before unavailable
----------------------------
DECLARE @AvailableToken TABLE  (
	[ExecutionTokenId] UniqueIdentifier NOT NULL
	,[LastGranteeId] int NULL
	,[TaskSecondaryId] int NOT NULL
	,[DateGranted] datetime NULL
	,[DateReturned] datetime NULL
	,[Status] tinyint NOT NULL
)

INSERT INTO @AvailableToken([ExecutionTokenId],[TaskSecondaryId],[DateGranted],[DateReturned],[Status])
SELECT TOP 1 [ExecutionTokenId]
	  ,ET.[TaskSecondaryId]
	  ,[DateGranted]
	  ,[DateReturned]
	  ,[Status]
FROM [PC].[ExecutionTokens] ET
WHERE [ET].[TaskSecondaryId] = @TaskSecondaryId 
AND ([Status] = 1
	OR
	(DATEDIFF(SECOND, [DateGranted], GETDATE()) > @SecondsOverride
		AND [Status] = 0)
	OR
	(DATEDIFF(SECOND, COALESCE([LastKeepAlive], '20150101'), GETDATE()) > @KeepAliveElapsedSeconds
		AND [Status] = 0)
	)
ORDER BY [Status] DESC;

----------------------------
-- If a viable token exists then update it as unavailable and return it
----------------------------
IF EXISTS(SELECT 1 FROM @AvailableToken)
BEGIN

	UPDATE [PC].[ExecutionTokens]
	SET [DateGranted] = GETDATE()
		,[DateReturned] = NULL
		,[Status] = 0
		,[TaskExecutionId] = @TaskExecutionId
	WHERE [ExecutionTokenId] = (SELECT TOP 1 [ExecutionTokenId] FROM @AvailableToken);

	SELECT TOP 1 [ExecutionTokenId]
            ,GETDATE() AS [StartedAt]
			,1 AS [Status]
	FROM @AvailableToken;

END
ELSE
BEGIN

	SELECT '00000000-0000-0000-0000-000000000000' AS [ExecutionTokenId]
            ,GETDATE() AS [StartedAt]
			,0 AS [Status];

END";

        public const string ReturnExecutionTokenQuery = @"
    UPDATE [PC].[ExecutionTokens] 
	SET [DateReturned] = GETDATE()
		,[Status] = 1
	WHERE [ExecutionTokenId] = @ExecutionTokenId
	AND [TaskExecutionId] = @TaskExecutionId;

    SELECT GETDATE() AS CompletedAt;";

        #endregion .: Executions :.

        
    }
}
