using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.CriticalSections
{
    public class TokensQueryBuilder
    {
        #region .: Override Query :.

        public const string OverrideBasedRequestCriticalSectionTokenQuery = @"
    ----------------------------
    -- Get an exclusive lock on the record
    ----------------------------
    UPDATE [Taskling].[CriticalSectionToken]
    SET [HoldLockTaskExecutionId] = @TaskExecutionId
    WHERE [TaskDefinitionId] = @TaskDefinitionId;

    IF NOT EXISTS(SELECT 1 FROM [Taskling].[CriticalSectionToken] WHERE [TaskDefinitionId] = @TaskDefinitionId)
	BEGIN
		INSERT INTO [Taskling].[CriticalSectionToken]([TaskDefinitionId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
		VALUES(@TaskDefinitionId, NULL, NULL, 1, 0)
	END

    ----------------------------
	-- If its status is available (1) or if it is unavailable but the override minutes have elapsed
	-- then update the record and return the token
	----------------------------
	IF EXISTS(SELECT 1 FROM [Taskling].[CriticalSectionToken] WHERE [TaskDefinitionId] = @TaskDefinitionId AND [Status] = 1)
		OR EXISTS(SELECT 1 FROM [Taskling].[CriticalSectionToken] WHERE [TaskDefinitionId] = @TaskDefinitionId AND DATEDIFF(SECOND, [DateGranted], GETUTCDATE()) > @SecondsOverride
														AND [Status] = 0)
	BEGIN
		UPDATE [Taskling].[CriticalSectionToken]
		SET [DateGranted] = GETUTCDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
		WHERE [TaskDefinitionId] = @TaskDefinitionId
	
		SELECT 1 AS [GrantStatus]
		
	END
	ELSE
	BEGIN
	
		SELECT 0 AS [GrantStatus];
	
	END";

        public const string OverrideBasedCriticalSectionQuery2 = @"
DECLARE @QueueIndexOfTaskExecution INT;

----------------------------
-- PHASE 1. GET EXCLUSIVE LOCKS ON ALL RELEVANT RECORDS
-- PREVENTS INCONSISTENT CHANGES AND PREVENTS DEADLOCKS
----------------------------
UPDATE CST 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[CriticalSectionToken] CST
WHERE [TaskDefinitionId] = @TaskDefinitionId;

UPDATE CSQ 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[CriticalSectionQueue] CSQ
WHERE [TaskDefinitionId] = @TaskDefinitionId;

-- CACHE DATA IN TEMP TABLES AND VARIABLES. PREVENTS DEADLOCKS BY NOT GOING BACK TO MAIN TABLES
SELECT [TaskExecutionId],[Status],[LastKeepAlive],[DateGranted]
INTO #ExecutionTokens
FROM Taskling.ExecutionToken WITH(NOLOCK)
WHERE [TaskDefinitionId] = @TaskDefinitionId

SELECT [CriticalSectionQueueIndex]
      ,[TaskDefinitionId]
      ,[TaskExecutionId]
INTO #CriticalSectionQueueOriginal
FROM [Taskling].[CriticalSectionQueue]
WHERE TaskDefinitionId = @TaskDefinitionId

SELECT TE.[TaskExecutionId]
INTO #CompletedAndQueuedExecutions
FROM [Taskling].[TaskExecution] TE WITH(NOLOCK)
JOIN #CriticalSectionQueueOriginal CSQ ON TE.TaskExecutionId = CSQ.TaskExecutionId
WHERE CompletedAt IS NOT NULL

DECLARE @CriticalSectionStatus INT = (SELECT TOP 1 [Status]
										FROM [Taskling].[CriticalSectionToken]
										WHERE TaskDefinitionId = @TaskDefinitionId)

-------------------------
-- PHASE 2. GET CRITICAL SECTION TOKEN AND QUEUE READY BY CLEANING ANY EXPIRED TASK EXECUTIONS
-------------------------

-- CREATE AN AVAILABLE CRITICAL SECTION TOKEN IF NONE EXISTS
-------------------------
IF (@CriticalSectionStatus IS NULL)
BEGIN
	INSERT INTO Taskling.[CriticalSectionToken]([TaskDefinitionId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
	VALUES(@TaskDefinitionId, NULL, NULL, 1, 0);
	
	SET @CriticalSectionStatus = 1;
END

-- REMOVE QUEUE ITEMS (TASK EXECUTIONS) THAT HAVE PASSED THE OVERRIDE TIME PERIOD
-------------------------
DELETE FROM Taskling.[CriticalSectionQueue]
WHERE TaskDefinitionId = @TaskDefinitionId
AND CriticalSectionQueueIndex IN (
    SELECT CriticalSectionQueueIndex
    FROM #CriticalSectionQueueOriginal CSQ
    JOIN #ExecutionTokens ET ON CSQ.TaskExecutionId = ET.TaskExecutionId
    WHERE CSQ.[TaskDefinitionId] = @TaskDefinitionId
    AND DATEDIFF(SECOND, ET.[DateGranted], GETUTCDATE()) > @SecondsOverride
	
    UNION

    SELECT CriticalSectionQueueIndex
    FROM #CriticalSectionQueueOriginal CSQ
    JOIN #CompletedAndQueuedExecutions ET ON CSQ.TaskExecutionId = ET.TaskExecutionId
);

SELECT [CriticalSectionQueueIndex]
      ,[TaskDefinitionId]
      ,[TaskExecutionId]
INTO #CriticalSectionQueue
FROM [Taskling].[CriticalSectionQueue]
WHERE TaskDefinitionId = @TaskDefinitionId

-- MAKE THE CRITICAL SECTION AVAILABLE IF THE ASSIGNED TASK EXECUTION HAS PASSED THE OVERRIDE TIME PERIOD OR IS COMPLETE
-------------------------
UPDATE CST
SET [DateReturned] = GETUTCDATE()
	,[Status] = 1
	,[TaskExecutionId] = 0
	,@CriticalSectionStatus = 1
FROM Taskling.[CriticalSectionToken] CST 
JOIN #ExecutionTokens ET ON CST.[TaskExecutionId] = ET.[TaskExecutionId]
WHERE CST.[TaskDefinitionId] = @TaskDefinitionId
AND (DATEDIFF(SECOND, ET.[DateGranted], GETUTCDATE()) > @SecondsOverride
    OR
    EXISTS( SELECT 1
            FROM [Taskling].[TaskExecution] TE WITH(NOLOCK)
            WHERE TaskExecutionId = CST.TaskExecutionId
            AND CompletedAt IS NOT NULL)
    );

--------------------------
-- IF TOKEN IS AVAILABLE
---- IF QUEUE IS EMPTY THEN GRANT
---- IF TASK EXECUTION IS NOT IN QUEUE THEN ADD IT AND DENY
---- IF TASK EXECUTION IS NEXT IN THE QUEUE THEN REMOVE IT FROM THE QUEUE AND GRANT
---- ELSE DENY
-- ELSE
---- IF TASK EXECUTION IS NOT IN QUEUE THEN ADD IT
---- DENY
--------------------------
IF (@CriticalSectionStatus = 1)
BEGIN

	SET @QueueIndexOfTaskExecution = (
				SELECT CriticalSectionQueueIndex FROM #CriticalSectionQueue
				WHERE [TaskExecutionId] = @TaskExecutionId);
	
	DECLARE @NextQueueItemIndex INT = (SELECT MIN(CriticalSectionQueueIndex)
					FROM #CriticalSectionQueue);
	
	-- IF NOTHING IN THE QUEUE			
	IF @NextQueueItemIndex IS NULL 
	BEGIN
		-- Grant the critical section to the task execution. 
		UPDATE CST
		SET [DateGranted] = GETUTCDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
        FROM Taskling.[CriticalSectionToken] CST 
		WHERE [TaskDefinitionId] = @TaskDefinitionId;
		
		SELECT 1 AS [GrantStatus];
	END
	-- IF THIS TASK IS NOT IN THE QUEUE THEN ADD IT
	ELSE IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO Taskling.[CriticalSectionQueue]
			   ([TaskDefinitionId]
			   ,[TaskExecutionId])
		VALUES
			   (@TaskDefinitionId
			   ,@TaskExecutionId);
			   
	    SELECT 0 AS [GrantStatus];
	END
	-- IF THIS TASK IS NEXT IN THE QUEUE
	ELSE IF @QueueIndexOfTaskExecution = @NextQueueItemIndex 
	BEGIN
		-- Remove the task execution from the queue
		DELETE FROM Taskling.[CriticalSectionQueue]
		WHERE CriticalSectionQueueIndex = @QueueIndexOfTaskExecution;
		
		-- Grant the critical section to the task execution. 
		UPDATE CST
		SET [DateGranted] = GETUTCDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
        FROM Taskling.[CriticalSectionToken] CST 
		WHERE [TaskDefinitionId] = @TaskDefinitionId;
		
		SELECT 1 AS [GrantStatus];
	END
	ELSE
	BEGIN							
		-- Deny 
		SELECT 0 AS [GrantStatus];
	END
END
ELSE
BEGIN

	SET @QueueIndexOfTaskExecution = (
				SELECT CriticalSectionQueueIndex FROM #CriticalSectionQueue
				WHERE [TaskExecutionId] = @TaskExecutionId);
	
	-- IF THIS TASK EXECUTION IS NOT IN THE QUEUE THEN ADD IT
	IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO Taskling.[CriticalSectionQueue]
			   ([TaskDefinitionId]
			   ,[TaskExecutionId])
		 VALUES
			   (@TaskDefinitionId
			   ,@TaskExecutionId);
	END
	
	-- Deny
	SELECT 0 AS [GrantStatus];

END";

        #endregion .: Override Query :.

        #region .: Keep Alive Query :.

        public const string KeepAliveBasedCriticalSectionQuery = @"
DECLARE @QueueIndexOfTaskExecution INT;

----------------------------
-- PHASE 1. GET EXCLUSIVE LOCKS ON ALL RELEVANT RECORDS
-- PREVENTS INCONSISTENT CHANGES AND PREVENTS DEADLOCKS
----------------------------
UPDATE CST 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[CriticalSectionToken] CST
WHERE [TaskDefinitionId] = @TaskDefinitionId;

UPDATE CSQ 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[CriticalSectionQueue] CSQ
WHERE [TaskDefinitionId] = @TaskDefinitionId;

-- CACHE DATA IN TEMP TABLES AND VARIABLES. PREVENTS DEADLOCKS BY NOT GOING BACK TO MAIN TABLES
SELECT [TaskExecutionId],[Status],[LastKeepAlive]
INTO #ExecutionTokens
FROM Taskling.ExecutionToken WITH(NOLOCK)
WHERE [TaskDefinitionId] = @TaskDefinitionId

SELECT [CriticalSectionQueueIndex]
      ,[TaskDefinitionId]
      ,[TaskExecutionId]
INTO #CriticalSectionQueueOriginal
FROM [Taskling].[CriticalSectionQueue]
WHERE TaskDefinitionId = @TaskDefinitionId

SELECT TE.[TaskExecutionId]
INTO #CompletedAndQueuedExecutions
FROM [Taskling].[TaskExecution] TE WITH(NOLOCK)
JOIN #CriticalSectionQueueOriginal CSQ ON TE.TaskExecutionId = CSQ.TaskExecutionId
WHERE CompletedAt IS NOT NULL

DECLARE @CriticalSectionStatus INT = (SELECT TOP 1 [Status]
										FROM [Taskling].[CriticalSectionToken]
										WHERE TaskDefinitionId = @TaskDefinitionId)

-------------------------
-- PHASE 2. GET CRITICAL SECTION TOKEN AND QUEUE READY BY CLEANING ANY EXPIRED TASK EXECUTIONS
-------------------------

-- CREATE AN AVAILABLE CRITICAL SECTION TOKEN IF NONE EXISTS
-------------------------
IF (@CriticalSectionStatus IS NULL)
BEGIN
	INSERT INTO Taskling.[CriticalSectionToken]([TaskDefinitionId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
	VALUES(@TaskDefinitionId, NULL, NULL, 1, 0);
	
	SET @CriticalSectionStatus = 1;
END

-- REMOVE QUEUE ITEMS (TASK EXECUTIONS) THAT HAVE EXPIRED KEEP ALIVES
-------------------------
DELETE FROM Taskling.[CriticalSectionQueue]
WHERE TaskDefinitionId = @TaskDefinitionId
AND CriticalSectionQueueIndex IN (
    SELECT CriticalSectionQueueIndex
    FROM #CriticalSectionQueueOriginal CSQ
    JOIN #ExecutionTokens ET ON CSQ.TaskExecutionId = ET.TaskExecutionId
    WHERE CSQ.[TaskDefinitionId] = @TaskDefinitionId
    AND DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds

    UNION

    SELECT CriticalSectionQueueIndex
    FROM #CriticalSectionQueueOriginal CSQ
    JOIN #CompletedAndQueuedExecutions ET ON CSQ.TaskExecutionId = ET.TaskExecutionId
);

SELECT [CriticalSectionQueueIndex]
      ,[TaskDefinitionId]
      ,[TaskExecutionId]
INTO #CriticalSectionQueue
FROM [Taskling].[CriticalSectionQueue]
WHERE TaskDefinitionId = @TaskDefinitionId

-- MAKE THE CRITICAL SECTION AVAILABLE IF THE ASSIGNED TASK EXECUTION HAS AN EXPIRED KEEP ALIVE OR IS COMPLETE
-------------------------
UPDATE CST
SET [DateReturned] = GETUTCDATE()
	,[Status] = 1
	,[TaskExecutionId] = 0
	,@CriticalSectionStatus = 1
FROM Taskling.[CriticalSectionToken] CST 
JOIN #ExecutionTokens ET ON CST.[TaskExecutionId] = ET.[TaskExecutionId]
WHERE CST.[TaskDefinitionId] = @TaskDefinitionId
AND (DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds
    OR
    EXISTS( SELECT 1
            FROM [Taskling].[TaskExecution] TE WITH(NOLOCK)
            WHERE TaskExecutionId = CST.TaskExecutionId
            AND CompletedAt IS NOT NULL)
    );

--------------------------
-- IF TOKEN IS AVAILABLE
---- IF QUEUE IS EMPTY THEN GRANT
---- IF TASK EXECUTION IS NOT IN QUEUE THEN ADD IT AND DENY
---- IF TASK EXECUTION IS NEXT IN THE QUEUE THEN REMOVE IT FROM THE QUEUE AND GRANT
---- ELSE DENY
-- ELSE
---- IF TASK EXECUTION IS NOT IN QUEUE THEN ADD IT
---- DENY
--------------------------
IF (@CriticalSectionStatus = 1)
BEGIN

	SET @QueueIndexOfTaskExecution = (
				SELECT CriticalSectionQueueIndex FROM #CriticalSectionQueue
				WHERE [TaskExecutionId] = @TaskExecutionId);
	
	DECLARE @NextQueueItemIndex INT = (SELECT MIN(CriticalSectionQueueIndex)
					FROM #CriticalSectionQueue);
	
	-- IF NOTHING IN THE QUEUE			
	IF @NextQueueItemIndex IS NULL 
	BEGIN
		-- Grant the critical section to the task execution. 
		UPDATE CST
		SET [DateGranted] = GETUTCDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
        FROM Taskling.[CriticalSectionToken] CST 
		WHERE [TaskDefinitionId] = @TaskDefinitionId;
		
		SELECT 1 AS [GrantStatus];
	END
	-- IF THIS TASK IS NOT IN THE QUEUE THEN ADD IT
	ELSE IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO Taskling.[CriticalSectionQueue]
			   ([TaskDefinitionId]
			   ,[TaskExecutionId])
		VALUES
			   (@TaskDefinitionId
			   ,@TaskExecutionId);
			   
	    SELECT 0 AS [GrantStatus];
	END
	-- IF THIS TASK IS NEXT IN THE QUEUE
	ELSE IF @QueueIndexOfTaskExecution = @NextQueueItemIndex 
	BEGIN
		-- Remove the task execution from the queue
		DELETE FROM Taskling.[CriticalSectionQueue]
		WHERE CriticalSectionQueueIndex = @QueueIndexOfTaskExecution;
		
		-- Grant the critical section to the task execution. 
		UPDATE CST
		SET [DateGranted] = GETUTCDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
        FROM Taskling.[CriticalSectionToken] CST 
		WHERE [TaskDefinitionId] = @TaskDefinitionId;
		
		SELECT 1 AS [GrantStatus];
	END
	ELSE
	BEGIN							
		-- Deny 
		SELECT 0 AS [GrantStatus];
	END
END
ELSE
BEGIN

	SET @QueueIndexOfTaskExecution = (
				SELECT CriticalSectionQueueIndex FROM #CriticalSectionQueue
				WHERE [TaskExecutionId] = @TaskExecutionId);
	
	-- IF THIS TASK EXECUTION IS NOT IN THE QUEUE THEN ADD IT
	IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO Taskling.[CriticalSectionQueue]
			   ([TaskDefinitionId]
			   ,[TaskExecutionId])
		 VALUES
			   (@TaskDefinitionId
			   ,@TaskExecutionId);
	END
	
	-- Deny
	SELECT 0 AS [GrantStatus];

END";

//@"DECLARE @QueueIndexOfTaskExecution INT;
//
//----------------------------
//-- PHASE 1. GET EXCLUSIVE LOCKS ON ALL RELEVANT RECORDS
//-- PREVENTS INCONSISTENT CHANGES AND PREVENTS DEADLOCKS
//----------------------------
//UPDATE [Taskling].[ExecutionToken] 
//SET [HoldLockTaskExecutionId] = @TaskExecutionId
//WHERE [TaskDefinitionId] = @TaskDefinitionId;
//
//UPDATE [Taskling].[CriticalSectionToken]
//SET [HoldLockTaskExecutionId] = @TaskExecutionId
//WHERE [TaskDefinitionId] = @TaskDefinitionId;
//
//UPDATE [Taskling].[CriticalSectionQueue]
//SET [HoldLockTaskExecutionId] = @TaskExecutionId
//WHERE [TaskDefinitionId] = @TaskDefinitionId;
//
//-------------------------
//-- PHASE 2. GET CRITICAL SECTION TOKEN AND QUEUE READY BY CLEANING ANY EXPIRED TASK EXECUTIONS
//-------------------------
//
//-- CREATE AN AVAILABLE CRITICAL SECTION TOKEN IF NONE EXISTS
//-------------------------
//IF NOT EXISTS(SELECT 1 FROM [Taskling].[CriticalSectionToken] WHERE [TaskDefinitionId] = @TaskDefinitionId)
//BEGIN
//	INSERT INTO [Taskling].[CriticalSectionToken]([TaskDefinitionId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
//	VALUES(@TaskDefinitionId, NULL, NULL, 1, 0);
//END
//
//-- REMOVE QUEUE ITEMS (TASK EXECUTIONS) THAT HAVE EXPIRED KEEP ALIVES
//-------------------------
//DELETE CSQ FROM [Taskling].[CriticalSectionQueue] CSQ
//JOIN [Taskling].[ExecutionToken] ET ON CSQ.TaskDefinitionId = ET.TaskDefinitionId
//WHERE ET.[TaskDefinitionId] = @TaskDefinitionId
//AND DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds;
//
//-- MAKE THE CRITICAL SECTION AVAILABLE IF THE ASSIGNED TASK EXECUTION HAS AN EXPIRED KEEP ALIVE
//-------------------------
//UPDATE CSC
//SET [DateReturned] = GETUTCDATE()
//	,[Status] = 1
//	,[TaskExecutionId] = 0
//FROM [Taskling].[CriticalSectionToken] CSC
//JOIN [Taskling].[ExecutionToken] ET ON CSC.TaskDefinitionId = ET.TaskDefinitionId
//	AND CSC.[TaskExecutionId] = ET.[TaskExecutionId]
//WHERE CSC.[TaskDefinitionId] = @TaskDefinitionId
//AND DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds;
//
//
//--------------------------
//-- IF TOKEN IS AVAILABLE
//---- IF QUEUE IS EMPTY THEN GRANT
//---- IF TASK EXECUTION IS NOT IN QUEUE THEN ADD IT AND DENY
//---- IF TASK EXECUTION IS NEXT IN THE QUEUE THEN REMOVE IT FROM THE QUEUE AND GRANT
//---- ELSE DENY
//-- ELSE
//---- IF TASK EXECUTION IS NOT IN QUEUE THEN ADD IT
//---- DENY
//--------------------------
//IF EXISTS(SELECT 1 FROM [Taskling].[CriticalSectionToken] 
//			WHERE [TaskDefinitionId] = @TaskDefinitionId 
//			AND [Status] = 1)
//BEGIN
//
//	SET @QueueIndexOfTaskExecution = (
//				SELECT CriticalSectionQueueIndex FROM [Taskling].[CriticalSectionQueue]
//				WHERE [TaskDefinitionId] = @TaskDefinitionId
//				AND [TaskExecutionId] = @TaskExecutionId);
//	
//	DECLARE @NextQueueItemIndex INT = (SELECT MIN(CriticalSectionQueueIndex)
//					FROM [Taskling].[CriticalSectionQueue]
//					WHERE [TaskDefinitionId] = @TaskDefinitionId);
//	
//	-- IF NOTHING IN THE QUEUE			
//	IF @NextQueueItemIndex IS NULL 
//	BEGIN
//		-- Grant the critical section to the task execution. 
//		UPDATE [Taskling].[CriticalSectionToken]
//		SET [DateGranted] = GETUTCDATE()
//			,[DateReturned] = NULL
//			,[Status] = 0
//			,[TaskExecutionId] = @TaskExecutionId
//		WHERE [TaskDefinitionId] = @TaskDefinitionId;
//		
//		SELECT 1 AS [GrantStatus];
//	END
//	-- IF THIS TASK IS NOT IN THE QUEUE THEN ADD IT
//	ELSE IF @QueueIndexOfTaskExecution IS NULL
//	BEGIN
//		INSERT INTO [Taskling].[CriticalSectionQueue]
//			   ([TaskDefinitionId]
//			   ,[TaskExecutionId])
//		VALUES
//			   (@TaskDefinitionId
//			   ,@TaskExecutionId);
//			   
//	    SELECT 0 AS [GrantStatus];
//	END
//	-- IF THIS TASK IS NEXT IN THE QUEUE
//	ELSE IF @QueueIndexOfTaskExecution = @NextQueueItemIndex 
//	BEGIN
//		-- Remove the task execution from the queue
//		DELETE FROM [Taskling].[CriticalSectionQueue]
//		WHERE CriticalSectionQueueIndex = @QueueIndexOfTaskExecution;
//		
//		-- Grant the critical section to the task execution. 
//		UPDATE [Taskling].[CriticalSectionToken]
//		SET [DateGranted] = GETUTCDATE()
//			,[DateReturned] = NULL
//			,[Status] = 0
//			,[TaskExecutionId] = @TaskExecutionId
//		WHERE [TaskDefinitionId] = @TaskDefinitionId;
//		
//		SELECT 1 AS [GrantStatus];
//	END
//	ELSE
//	BEGIN							
//		-- Deny 
//		SELECT 0 AS [GrantStatus];
//	END
//END
//ELSE
//BEGIN
//
//	SET @QueueIndexOfTaskExecution = (
//				SELECT CriticalSectionQueueIndex FROM [Taskling].[CriticalSectionQueue]
//				WHERE [TaskDefinitionId] = @TaskDefinitionId
//				AND [TaskExecutionId] = @TaskExecutionId);
//	
//	-- IF THIS TASK EXECUTION IS NOT IN THE QUEUE THEN ADD IT
//	IF @QueueIndexOfTaskExecution IS NULL
//	BEGIN
//		INSERT INTO [Taskling].[CriticalSectionQueue]
//			   ([TaskDefinitionId]
//			   ,[TaskExecutionId])
//		 VALUES
//			   (@TaskDefinitionId
//			   ,@TaskExecutionId);
//	END
//	
//	-- Deny
//	SELECT 0 AS [GrantStatus];
//
//END";

        public const string ReturnCriticalSectionTokenQuery = @"UPDATE [Taskling].[CriticalSectionToken] 
	SET [DateReturned] = GETUTCDATE()
		,[Status] = 1
	WHERE [TaskDefinitionId] = @TaskDefinitionId
	AND [TaskExecutionId] = @TaskExecutionId;";

        #endregion .: Keep ALive Query :.
    }
}
