using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.TaskExecution.QueryBuilders
{
    internal class TokensQueryBuilder
    {
        #region .: Base Queries :.

        public const string OverrideBasedRequestExecutionTokenQuery = @"
    ----------------------------
-- Get an exclusive lock on the records of this process
----------------------------
UPDATE ET
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM  [Taskling].[ExecutionToken] ET 
WHERE [TaskDefinitionId] = @TaskDefinitionId;

----------------------------------------------
-- CREATE AN AVAILABLE EXECUTION TOKEN IF NONE EXISTS
----------------------------------------------

DECLARE @TokenCount INT = (SELECT COUNT(*)
							FROM [Taskling].[ExecutionToken]
							WHERE TaskDefinitionId = @TaskDefinitionId)

IF (@TokenCount = 0)
BEGIN
	INSERT INTO [Taskling].[ExecutionToken]([TaskDefinitionId],[Status],[LastKeepAlive])
    VALUES (@TaskDefinitionId, 1, GETUTCDATE())
END

----------------------------------------------
-- ADD TOKENS TO TEMP TABLE TO AVOID DEADLOCKS
----------------------------------------------

SELECT [ExecutionTokenId]
	  ,[TaskDefinitionId]
	  ,[DateGranted]
	  ,[DateReturned]
	  ,[Status]
INTO #ExecutionToken
FROM Taskling.[ExecutionToken] ET
WHERE [TaskDefinitionId] = @TaskDefinitionId;

----------------------------
-- Get 
-- an available token into a temporary table
-- or an unavailable token which can be overridden
-- but take available tokens before unavailable
----------------------------
DECLARE @AvailableToken TABLE  (
	[ExecutionTokenId] int NOT NULL
	,[LastGranteeId] int NULL
	,[TaskDefinitionId] int NOT NULL
	,[DateGranted] datetime NULL
	,[DateReturned] datetime NULL
	,[Status] tinyint NOT NULL
)

INSERT INTO @AvailableToken([ExecutionTokenId],[TaskDefinitionId],[DateGranted],[DateReturned],[Status])
SELECT TOP 1 [ExecutionTokenId]
	  ,[TaskDefinitionId]
	  ,[DateGranted]
	  ,[DateReturned]
	  ,[Status]
FROM #ExecutionToken
WHERE [TaskDefinitionId] = @TaskDefinitionId 
AND [Status] = 1
	OR
	([Status] = 0
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
		,[Status] = 0
		,[TaskExecutionId] = @TaskExecutionId
    FROM [Taskling].[ExecutionToken] ET
	WHERE ET.TaskDefinitionId = @TaskDefinitionId
    AND ET.ExecutionTokenId = (SELECT TOP 1 ExecutionTokenId FROM @AvailableToken)

	SELECT TOP 1 [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,1 AS [GrantStatus]
	FROM @AvailableToken;

END
ELSE
BEGIN

	SELECT 0 AS [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,0 AS [GrantStatus];

END
";

        public const string KeepAliveBasedRequestExecutionTokenQuery = @"----------------------------
-- Get an exclusive lock on the records of this process
----------------------------
UPDATE ET
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[ExecutionToken] ET
WHERE [TaskDefinitionId] = @TaskDefinitionId;

----------------------------------------------
-- CREATE AN AVAILABLE EXECUTION TOKEN IF NONE EXISTS
----------------------------------------------

DECLARE @TokenCount INT = (SELECT COUNT(*)
							FROM [Taskling].[ExecutionToken]
							WHERE TaskDefinitionId = @TaskDefinitionId)

IF (@TokenCount = 0)
BEGIN
	INSERT INTO [Taskling].[ExecutionToken]([TaskDefinitionId],[Status],[LastKeepAlive])
    VALUES (@TaskDefinitionId, 1, GETUTCDATE())
END

----------------------------------------------
-- ADD TOKENS TO TEMP TABLE TO AVOID DEADLOCKS
----------------------------------------------

SELECT [ExecutionTokenId]
	  ,[TaskDefinitionId]
	  ,[DateGranted]
	  ,[DateReturned]
	  ,[Status]
      ,[LastKeepAlive]
INTO #ExecutionToken
FROM Taskling.[ExecutionToken] ET
WHERE [TaskDefinitionId] = @TaskDefinitionId;

----------------------------
-- Get 
-- an available token into a temporary table
-- or an unavailable token which can be overridden
-- but take available tokens before unavailable
----------------------------
DECLARE @AvailableToken TABLE  (
	[ExecutionTokenId] int NOT NULL
	,[LastGranteeId] int NULL
	,[TaskDefinitionId] int NOT NULL
	,[DateGranted] datetime NULL
	,[DateReturned] datetime NULL
	,[Status] tinyint NOT NULL
)

INSERT INTO @AvailableToken([ExecutionTokenId],[TaskDefinitionId],[DateGranted],[DateReturned],[Status])
SELECT TOP 1 [ExecutionTokenId]
	  ,[TaskDefinitionId]
	  ,[DateGranted]
	  ,[DateReturned]
	  ,[Status]
FROM #ExecutionToken
WHERE [Status] = 1
	OR ([Status] = 0 
		AND DATEDIFF(SECOND, COALESCE([LastKeepAlive], '20150101'), GETUTCDATE()) > @KeepAliveElapsedSeconds)
ORDER BY [Status] DESC;

----------------------------
-- If a viable token exists then update it as unavailable and return it
----------------------------
IF EXISTS(SELECT 1 FROM @AvailableToken)
BEGIN

    UPDATE ET
	SET [DateGranted] = GETUTCDATE()
		,[DateReturned] = NULL
		,[Status] = 0
		,[TaskExecutionId] = @TaskExecutionId
    FROM Taskling.[ExecutionToken] ET 
	WHERE ET.TaskDefinitionId = @TaskDefinitionId
    AND ET.ExecutionTokenId = (SELECT TOP 1 ExecutionTokenId FROM @AvailableToken)

	SELECT TOP 1 [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,1 AS [GrantStatus]
	FROM @AvailableToken;

END
ELSE
BEGIN

	SELECT 0 AS [ExecutionTokenId]
            ,GETUTCDATE() AS [StartedAt]
			,0 AS [GrantStatus];

END";

        public const string ReturnExecutionTokenQuery = @"
    UPDATE [Taskling].[ExecutionToken] 
	SET [DateReturned] = GETUTCDATE()
		,[Status] = 1
	WHERE [TaskDefinitionId] = @TaskDefinitionId
    AND [ExecutionTokenId] = @ExecutionTokenId
	AND [TaskExecutionId] = @TaskExecutionId;

    SELECT GETUTCDATE() AS CompletedAt;";

        public const string GetCurrentDateQuery = @"SELECT GETUTCDATE() AS CurrentDate;";

        #endregion .: Executions :.


    }
}
