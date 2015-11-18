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
    UPDATE {0}.[CriticalSectionTokens]
    SET [HoldLockTaskExecutionId] = @TaskExecutionId
    WHERE [TaskSecondaryId] = @TaskSecondaryId;

    IF NOT EXISTS(SELECT 1 FROM {0}.[CriticalSectionTokens] WHERE [TaskSecondaryId] = @TaskSecondaryId)
	BEGIN
		INSERT INTO {0}.[CriticalSectionTokens]([TaskSecondaryId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
		VALUES(@TaskSecondaryId, NULL, NULL, 1, 0)
	END

    ----------------------------
	-- If its status is available (1) or if it is unavailable but the override minutes have elapsed
	-- then update the record and return the token
	----------------------------
	IF EXISTS(SELECT 1 FROM {0}.[CriticalSectionTokens] WHERE [TaskSecondaryId] = @TaskSecondaryId AND [Status] = 1)
		OR EXISTS(SELECT 1 FROM {0}.[CriticalSectionTokens] WHERE [TaskSecondaryId] = @TaskSecondaryId AND DATEDIFF(SECOND, [DateGranted], GETDATE()) > @SecondsOverride
														AND [Status] = 0)
	BEGIN
		UPDATE {0}.[CriticalSectionTokens]
		SET [DateGranted] = GETDATE()
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

        private const string KeepAliveBasedCriticalSectionQuery = @"DECLARE @QueueIndexOfTaskExecution INT;

----------------------------
-- PHASE 1. GET EXCLUSIVE LOCKS ON ALL RELEVANT RECORDS
-- PREVENTS INCONSISTENT CHANGES AND PREVENTS DEADLOCKS
----------------------------
UPDATE {0}.[ExecutionTokens] 
SET [HoldLockTaskExecutionId] = @TaskExecutionId
WHERE [TaskSecondaryId] = @TaskSecondaryId;

UPDATE {0}.[CriticalSectionTokens]
SET [HoldLockTaskExecutionId] = @TaskExecutionId
WHERE [TaskSecondaryId] = @TaskSecondaryId;

UPDATE {0}.[CriticalSectionQueue]
SET [HoldLockTaskExecutionId] = @TaskExecutionId
WHERE [TaskSecondaryId] = @TaskSecondaryId;

-------------------------
-- PHASE 2. GET CRITICAL SECTION TOKEN AND QUEUE READY BY CLEANING ANY EXPIRED TASK EXECUTIONS
-------------------------

-- CREATE AN AVAILABLE CRITICAL SECTION TOKEN IF NONE EXISTS
-------------------------
IF NOT EXISTS(SELECT 1 FROM {0}.[CriticalSectionTokens] WHERE [TaskSecondaryId] = @TaskSecondaryId)
BEGIN
	INSERT INTO {0}.[CriticalSectionTokens]([TaskSecondaryId],[DateGranted],[DateReturned],[Status],[TaskExecutionId])
	VALUES(@TaskSecondaryId, NULL, NULL, 1, 0);
END

-- REMOVE QUEUE ITEMS (TASK EXECUTIONS) THAT HAVE EXPIRED KEEP ALIVES
-------------------------
DELETE CSQ FROM {0}.[CriticalSectionQueue] CSQ
JOIN {0}.[ExecutionTokens] ET ON CSQ.TaskSecondaryId = ET.TaskSecondaryId
WHERE ET.[TaskSecondaryId] = @TaskSecondaryId
AND DATEDIFF(SECOND, ET.LastKeepAlive, GETDATE()) > @KeepAliveElapsedSeconds;

-- MAKE THE CRITICAL SECTION AVAILABLE IF THE ASSIGNED TASK EXECUTION HAS AN EXPIRED KEEP ALIVE
-------------------------
UPDATE CSC
SET [DateReturned] = GETDATE()
	,[Status] = 1
	,[TaskExecutionId] = 0
FROM {0}.[CriticalSectionTokens] CSC
JOIN {0}.[ExecutionTokens] ET ON CSC.TaskSecondaryId = ET.TaskSecondaryId
	AND CSC.[TaskExecutionId] = ET.[TaskExecutionId]
WHERE CSC.[TaskSecondaryId] = @TaskSecondaryId
AND DATEDIFF(SECOND, ET.LastKeepAlive, GETDATE()) > @KeepAliveElapsedSeconds;


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
IF EXISTS(SELECT 1 FROM {0}.[CriticalSectionTokens] 
			WHERE [TaskSecondaryId] = @TaskSecondaryId 
			AND [Status] = 1)
BEGIN

	SET @QueueIndexOfTaskExecution = (
				SELECT CriticalSectionQueueIndex FROM {0}.[CriticalSectionQueue]
				WHERE [TaskSecondaryId] = @TaskSecondaryId
				AND [TaskExecutionId] = @TaskExecutionId);
	
	DECLARE @NextQueueItemIndex INT = (SELECT MIN(CriticalSectionQueueIndex)
					FROM {0}.[CriticalSectionQueue]
					WHERE [TaskSecondaryId] = @TaskSecondaryId);
	
	-- IF NOTHING IN THE QUEUE			
	IF @NextQueueItemIndex IS NULL 
	BEGIN
		-- Grant the critical section to the task execution. 
		UPDATE {0}.[CriticalSectionTokens]
		SET [DateGranted] = GETDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
		WHERE [TaskSecondaryId] = @TaskSecondaryId;
		
		SELECT 1 AS [GrantStatus];
	END
	-- IF THIS TASK IS NOT IN THE QUEUE THEN ADD IT
	ELSE IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO {0}.[CriticalSectionQueue]
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
		DELETE FROM {0}.[CriticalSectionQueue]
		WHERE CriticalSectionQueueIndex = @QueueIndexOfTaskExecution;
		
		-- Grant the critical section to the task execution. 
		UPDATE {0}.[CriticalSectionTokens]
		SET [DateGranted] = GETDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[TaskExecutionId] = @TaskExecutionId
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
				SELECT CriticalSectionQueueIndex FROM {0}.[CriticalSectionQueue]
				WHERE [TaskSecondaryId] = @TaskSecondaryId
				AND [TaskExecutionId] = @TaskExecutionId);
	
	-- IF THIS TASK EXECUTION IS NOT IN THE QUEUE THEN ADD IT
	IF @QueueIndexOfTaskExecution IS NULL
	BEGIN
		INSERT INTO {0}.[CriticalSectionQueue]
			   ([TaskSecondaryId]
			   ,[TaskExecutionId])
		 VALUES
			   (@TaskSecondaryId
			   ,@TaskExecutionId);
	END
	
	-- Deny
	SELECT 0 AS [GrantStatus];

END";

        private const string ReturnCriticalSectionTokenQuery = @"UPDATE {0}.[CriticalSectionTokens] 
	SET [DateReturned] = GETDATE()
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
