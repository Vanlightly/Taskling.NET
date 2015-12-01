using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.CriticalSections
{
    public class TokensQueryBuilder
    {
        private const string OverrideBasedRequestCriticalSectionTokenQuery = @"
    ----------------------------
    -- Get an exclusive lock on the record
    ----------------------------
    UPDATE {0}.[CriticalSectionToken]
    SET [HoldLockTaskExecutionId] = @TaskExecutionId
    WHERE [TaskSecondaryId] = @TaskSecondaryId;

    IF NOT EXISTS(SELECT 1 FROM {0}.[CriticalSectionToken] WHERE [TaskSecondaryId] = @TaskSecondaryId)
	BEGIN
		INSERT INTO {0}.[CriticalSectionToken]([TaskSecondaryId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
		VALUES(@TaskSecondaryId, NULL, NULL, 1, 0)
	END

    ----------------------------
	-- If its status is available (1) or if it is unavailable but the override minutes have elapsed
	-- then update the record and return the token
	----------------------------
	IF EXISTS(SELECT 1 FROM {0}.[CriticalSectionToken] WHERE [TaskSecondaryId] = @TaskSecondaryId AND [Status] = 1)
		OR EXISTS(SELECT 1 FROM {0}.[CriticalSectionToken] WHERE [TaskSecondaryId] = @TaskSecondaryId AND DATEDIFF(SECOND, [DateGranted], GETUTCDATE()) > @SecondsOverride
														AND [Status] = 0)
	BEGIN
		UPDATE {0}.[CriticalSectionToken]
		SET [DateGranted] = GETUTCDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
		WHERE [TaskSecondaryId] = @TaskSecondaryId
	
		SELECT 1 AS [GrantStatus]
		
	END
	ELSE
	BEGIN
	
		SELECT 0 AS [GrantStatus];
	
	END";

        private const string KeepAliveBasedCriticalSectionQuery = @"
DECLARE @QueueIndexOfTaskExecution INT;

----------------------------
-- PHASE 1. GET EXCLUSIVE LOCKS ON ALL RELEVANT RECORDS
-- PREVENTS INCONSISTENT CHANGES AND PREVENTS DEADLOCKS
----------------------------
UPDATE ET 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[ExecutionToken] ET WITH(INDEX(IX_ExecutionToken_TaskSecondaryId))
WHERE [TaskSecondaryId] = @TaskSecondaryId;

UPDATE CST 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[CriticalSectionToken] CST
WHERE [TaskSecondaryId] = @TaskSecondaryId;

UPDATE CSQ 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
FROM Taskling.[CriticalSectionQueue] CSQ WITH(INDEX(IX_CriticalSectionQueue_TaskSecondaryId))
WHERE [TaskSecondaryId] = @TaskSecondaryId;

-- CACHE DATA IN TEMP TABLES AND VARIABLES. PREVENTS DEADLOCKS BY NOT GOING BACK TO MAIN TABLES
SELECT [TaskExecutionId],[Status],[LastKeepAlive]
INTO #ExecutionTokens
FROM Taskling.ExecutionToken WITH (INDEX(IX_ExecutionToken_TaskSecondaryId))
WHERE [TaskSecondaryId] = @TaskSecondaryId

DECLARE @CriticalSectionStatus INT = (SELECT TOP 1 [Status]
										FROM [TasklingDb].[Taskling].[CriticalSectionToken]
										WHERE TaskSecondaryId = @TaskSecondaryId)

-------------------------
-- PHASE 2. GET CRITICAL SECTION TOKEN AND QUEUE READY BY CLEANING ANY EXPIRED TASK EXECUTIONS
-------------------------

-- CREATE AN AVAILABLE CRITICAL SECTION TOKEN IF NONE EXISTS
-------------------------
IF (@CriticalSectionStatus IS NULL)
BEGIN
	INSERT INTO Taskling.[CriticalSectionToken]([TaskSecondaryId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
	VALUES(@TaskSecondaryId, NULL, NULL, 1, 0);
	
	SET @CriticalSectionStatus = 1;
END

-- REMOVE QUEUE ITEMS (TASK EXECUTIONS) THAT HAVE EXPIRED KEEP ALIVES
-------------------------
DELETE CSQ FROM Taskling.[CriticalSectionQueue] CSQ WITH(INDEX(IX_CriticalSectionQueue_TaskSecondaryId))
JOIN #ExecutionTokens ET ON 1=1
WHERE CSQ.[TaskSecondaryId] = @TaskSecondaryId
AND DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds;

SELECT [CriticalSectionQueueIndex]
      ,[TaskSecondaryId]
      ,[TaskExecutionId]
INTO #CriticalSectionQueue
FROM [Taskling].[CriticalSectionQueue] WITH (INDEX(IX_CriticalSectionQueue_TaskSecondaryId))
WHERE TaskSecondaryId = @TaskSecondaryId

-- MAKE THE CRITICAL SECTION AVAILABLE IF THE ASSIGNED TASK EXECUTION HAS AN EXPIRED KEEP ALIVE
-------------------------
UPDATE CST
SET [DateReturned] = GETUTCDATE()
	,[Status] = 1
	,[TaskExecutionId] = 0
	,@CriticalSectionStatus = 1
FROM Taskling.[CriticalSectionToken] CST 
JOIN #ExecutionTokens ET ON CST.[TaskExecutionId] = ET.[TaskExecutionId]
WHERE CST.[TaskSecondaryId] = @TaskSecondaryId
AND DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds;

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
		WHERE [TaskSecondaryId] = @TaskSecondaryId;
		
		SELECT 1 AS [GrantStatus];
	END
	-- IF THIS TASK IS NOT IN THE QUEUE THEN ADD IT
	ELSE IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO Taskling.[CriticalSectionQueue]
			   ([TaskSecondaryId]
			   ,[TaskExecutionId])
		VALUES
			   (@TaskSecondaryId
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
		WHERE [TaskSecondaryId] = @TaskSecondaryId;
		
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
			   ([TaskSecondaryId]
			   ,[TaskExecutionId])
		 VALUES
			   (@TaskSecondaryId
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
//UPDATE {0}.[ExecutionToken] 
//SET [HoldLockTaskExecutionId] = @TaskExecutionId
//WHERE [TaskSecondaryId] = @TaskSecondaryId;
//
//UPDATE {0}.[CriticalSectionToken]
//SET [HoldLockTaskExecutionId] = @TaskExecutionId
//WHERE [TaskSecondaryId] = @TaskSecondaryId;
//
//UPDATE {0}.[CriticalSectionQueue]
//SET [HoldLockTaskExecutionId] = @TaskExecutionId
//WHERE [TaskSecondaryId] = @TaskSecondaryId;
//
//-------------------------
//-- PHASE 2. GET CRITICAL SECTION TOKEN AND QUEUE READY BY CLEANING ANY EXPIRED TASK EXECUTIONS
//-------------------------
//
//-- CREATE AN AVAILABLE CRITICAL SECTION TOKEN IF NONE EXISTS
//-------------------------
//IF NOT EXISTS(SELECT 1 FROM {0}.[CriticalSectionToken] WHERE [TaskSecondaryId] = @TaskSecondaryId)
//BEGIN
//	INSERT INTO {0}.[CriticalSectionToken]([TaskSecondaryId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
//	VALUES(@TaskSecondaryId, NULL, NULL, 1, 0);
//END
//
//-- REMOVE QUEUE ITEMS (TASK EXECUTIONS) THAT HAVE EXPIRED KEEP ALIVES
//-------------------------
//DELETE CSQ FROM {0}.[CriticalSectionQueue] CSQ
//JOIN {0}.[ExecutionToken] ET ON CSQ.TaskSecondaryId = ET.TaskSecondaryId
//WHERE ET.[TaskSecondaryId] = @TaskSecondaryId
//AND DATEDIFF(SECOND, ET.LastKeepAlive, GETUTCDATE()) > @KeepAliveElapsedSeconds;
//
//-- MAKE THE CRITICAL SECTION AVAILABLE IF THE ASSIGNED TASK EXECUTION HAS AN EXPIRED KEEP ALIVE
//-------------------------
//UPDATE CSC
//SET [DateReturned] = GETUTCDATE()
//	,[Status] = 1
//	,[TaskExecutionId] = 0
//FROM {0}.[CriticalSectionToken] CSC
//JOIN {0}.[ExecutionToken] ET ON CSC.TaskSecondaryId = ET.TaskSecondaryId
//	AND CSC.[TaskExecutionId] = ET.[TaskExecutionId]
//WHERE CSC.[TaskSecondaryId] = @TaskSecondaryId
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
//IF EXISTS(SELECT 1 FROM {0}.[CriticalSectionToken] 
//			WHERE [TaskSecondaryId] = @TaskSecondaryId 
//			AND [Status] = 1)
//BEGIN
//
//	SET @QueueIndexOfTaskExecution = (
//				SELECT CriticalSectionQueueIndex FROM {0}.[CriticalSectionQueue]
//				WHERE [TaskSecondaryId] = @TaskSecondaryId
//				AND [TaskExecutionId] = @TaskExecutionId);
//	
//	DECLARE @NextQueueItemIndex INT = (SELECT MIN(CriticalSectionQueueIndex)
//					FROM {0}.[CriticalSectionQueue]
//					WHERE [TaskSecondaryId] = @TaskSecondaryId);
//	
//	-- IF NOTHING IN THE QUEUE			
//	IF @NextQueueItemIndex IS NULL 
//	BEGIN
//		-- Grant the critical section to the task execution. 
//		UPDATE {0}.[CriticalSectionToken]
//		SET [DateGranted] = GETUTCDATE()
//			,[DateReturned] = NULL
//			,[Status] = 0
//			,[TaskExecutionId] = @TaskExecutionId
//		WHERE [TaskSecondaryId] = @TaskSecondaryId;
//		
//		SELECT 1 AS [GrantStatus];
//	END
//	-- IF THIS TASK IS NOT IN THE QUEUE THEN ADD IT
//	ELSE IF @QueueIndexOfTaskExecution IS NULL
//	BEGIN
//		INSERT INTO {0}.[CriticalSectionQueue]
//			   ([TaskSecondaryId]
//			   ,[TaskExecutionId])
//		VALUES
//			   (@TaskSecondaryId
//			   ,@TaskExecutionId);
//			   
//	    SELECT 0 AS [GrantStatus];
//	END
//	-- IF THIS TASK IS NEXT IN THE QUEUE
//	ELSE IF @QueueIndexOfTaskExecution = @NextQueueItemIndex 
//	BEGIN
//		-- Remove the task execution from the queue
//		DELETE FROM {0}.[CriticalSectionQueue]
//		WHERE CriticalSectionQueueIndex = @QueueIndexOfTaskExecution;
//		
//		-- Grant the critical section to the task execution. 
//		UPDATE {0}.[CriticalSectionToken]
//		SET [DateGranted] = GETUTCDATE()
//			,[DateReturned] = NULL
//			,[Status] = 0
//			,[TaskExecutionId] = @TaskExecutionId
//		WHERE [TaskSecondaryId] = @TaskSecondaryId;
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
//				SELECT CriticalSectionQueueIndex FROM {0}.[CriticalSectionQueue]
//				WHERE [TaskSecondaryId] = @TaskSecondaryId
//				AND [TaskExecutionId] = @TaskExecutionId);
//	
//	-- IF THIS TASK EXECUTION IS NOT IN THE QUEUE THEN ADD IT
//	IF @QueueIndexOfTaskExecution IS NULL
//	BEGIN
//		INSERT INTO {0}.[CriticalSectionQueue]
//			   ([TaskSecondaryId]
//			   ,[TaskExecutionId])
//		 VALUES
//			   (@TaskSecondaryId
//			   ,@TaskExecutionId);
//	END
//	
//	-- Deny
//	SELECT 0 AS [GrantStatus];
//
//END";

        private const string ReturnCriticalSectionTokenQuery = @"UPDATE {0}.[CriticalSectionToken] 
	SET [DateReturned] = GETUTCDATE()
		,[Status] = 1
	WHERE [TaskSecondaryId] = @TaskSecondaryId
	AND [TaskExecutionId] = @TaskExecutionId;";

        public static string GetOverrideBasedRequestCriticalSectionTokenQuery(string tableSchema)
        {
            return string.Format(OverrideBasedRequestCriticalSectionTokenQuery, tableSchema);
        }

        public static string GetKeepAliveBasedCriticalSectionQuery(string tableSchema)
        {
            return string.Format(KeepAliveBasedCriticalSectionQuery, tableSchema);
        }

        public static string GetReturnCriticalSectionTokenQuery(string tableSchema)
        {
            return string.Format(ReturnCriticalSectionTokenQuery, tableSchema);
        }
    }
}
