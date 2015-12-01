using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;

namespace Taskling.SqlServer.TaskExecution
{
    internal class TokensQueryBuilder
    {
        #region .: Base Queries :.

        private const string OverrideBasedRequestExecutionTokenQuery = @"
    ----------------------------
-- Get an exclusive lock on the records of this process
----------------------------
UPDATE ET
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM  {0}.[ExecutionToken] ET WITH(INDEX(IX_ExecutionToken_TaskSecondaryId))
WHERE [TaskSecondaryId] = @TaskSecondaryId;

----------------------------
-- Get 
-- an available token into a temporary table
-- or an unavailable token which can be overridden
-- but take available tokens before unavailable
----------------------------
DECLARE @AvailableToken TABLE  (
	[ExecutionTokenId] int NOT NULL
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
FROM {0}.[ExecutionToken]
WHERE [TaskSecondaryId] = @TaskSecondaryId 
AND ([Status] = 1 OR [Status] = 3
	OR
	([Status] = 0)
        AND DATEDIFF(SECOND, [DateGranted], GETUTCDATE()) > @SecondsOverride)
ORDER BY [Status] DESC;

----------------------------
-- If a viable token exists then update it as unavailable and return it
----------------------------
IF EXISTS(SELECT 1 FROM @AvailableToken)
BEGIN

	UPDATE ET
	SET [DateGranted] = GETUTCDATE()
		,[DateReturned] = NULL
		,[Status] = CASE AT.[Status] WHEN 3 THEN 3 ELSE 0 END
		,[TaskExecutionId] = @TaskExecutionId
    FROM {0}.[ExecutionToken] ET
	JOIN @AvailableToken AS AT ON ET.[ExecutionTokenId] = AT.[ExecutionTokenId];

	SELECT TOP 1 [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,CASE [Status] WHEN 3 THEN 2 ELSE 1 END AS [GrantStatus]
	FROM @AvailableToken;

END
ELSE
BEGIN

	SELECT 0 AS [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,0 AS [GrantStatus];

END
";

        private const string KeepAliveBasedRequestExecutionTokenQuery = @"----------------------------
-- Get an exclusive lock on the records of this process
----------------------------
UPDATE ET
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[ExecutionToken] ET WITH (INDEX(IX_ExecutionToken_TaskSecondaryId))
WHERE [TaskSecondaryId] = @TaskSecondaryId;

----------------------------
-- Get 
-- an available token into a temporary table
-- or an unavailable token which can be overridden
-- but take available tokens before unavailable
----------------------------
DECLARE @AvailableToken TABLE  (
	[ExecutionTokenId] int NOT NULL
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
FROM Taskling.[ExecutionToken] ET WITH (INDEX(IX_ExecutionToken_ForKeepAliveQuery))
WHERE [ET].[TaskSecondaryId] = @TaskSecondaryId 
AND ([Status] = 1 OR [Status] = 3
	OR
	([Status] = 0 
		AND DATEDIFF(SECOND, COALESCE([LastKeepAlive], '20150101'), GETUTCDATE()) > @KeepAliveElapsedSeconds)
	)
ORDER BY [Status] DESC;

----------------------------
-- If a viable token exists then update it as unavailable and return it
----------------------------
IF EXISTS(SELECT 1 FROM @AvailableToken)
BEGIN

    UPDATE ET
	SET [DateGranted] = GETUTCDATE()
		,[DateReturned] = NULL
		,[Status] = CASE AT.[Status] WHEN 3 THEN 3 ELSE 0 END
		,[TaskExecutionId] = @TaskExecutionId
    FROM Taskling.[ExecutionToken] ET 
	JOIN @AvailableToken AS AT ON ET.[ExecutionTokenId] = AT.[ExecutionTokenId];

	SELECT TOP 1 [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,CASE [Status] WHEN 3 THEN 2 ELSE 1 END AS [GrantStatus]
	FROM @AvailableToken;

END
ELSE
BEGIN

	SELECT 0 AS [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,0 AS [GrantStatus];

END";

        private const string ReturnExecutionTokenQuery = @"
    UPDATE {0}.[ExecutionToken] 
	SET [DateReturned] = GETUTCDATE()
		,[Status] = 1
	WHERE [ExecutionTokenId] = @ExecutionTokenId
	AND [TaskExecutionId] = @TaskExecutionId;

    SELECT GETUTCDATE() AS CompletedAt;";

        public const string GetCurrentDateQuery = @"SELECT GETUTCDATE() AS CurrentDate;";

        #endregion .: Executions :.

        public static string GetOverrideBasedRequestExecutionTokenQuery(string tableSchema)
        {
            return string.Format(OverrideBasedRequestExecutionTokenQuery, tableSchema);
        }

        public static string GetKeepAliveBasedRequestExecutionTokenQuery(string tableSchema)
        {
            return string.Format(KeepAliveBasedRequestExecutionTokenQuery, tableSchema);
        }

        public static string GetReturnExecutionTokenQuery(string tableSchema)
        {
            return string.Format(ReturnExecutionTokenQuery, tableSchema);
        }
    }
}
