using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Taskling.SqlServer.CriticalSections
{
    public class TokensQueryBuilder
    {
        public const string RequestCriticalSectionTokenQuery = @"
    ----------------------------
    -- Get an exclusive lock on the record
    ----------------------------
    UPDATE [PC].[CriticalSectionTokens]
    SET [HoldLockExecutionId] = @ExecutionId
    WHERE [TaskId] = @TaskId;

    IF NOT EXISTS(SELECT 1 FROM [PC].[CriticalSectionTokens] WHERE [TaskId] = @TaskId)
	BEGIN
		INSERT INTO [PC].[CriticalSectionTokens]([TaskId],[DateGranted],[DateReturned],[Status],[ExecutionId])
		VALUES(@TaskId, NULL, NULL, 1, '00000000-0000-0000-0000-000000000000')
	END

    ----------------------------
	-- If its status is available (1) or if it is unavailable but the override minutes have elapsed
	-- then update the record and return the token
	----------------------------
	IF EXISTS(SELECT 1 FROM [PC].[CriticalSectionTokens] WHERE [TaskId] = @TaskId AND [Status] = 1)
		OR EXISTS(SELECT 1 FROM [PC].[CriticalSectionTokens] WHERE [TaskId] = @TaskId AND DATEDIFF(SECOND, [DateGranted], GETDATE()) > @SecondsOverride
														AND [Status] = 0)
	BEGIN
		UPDATE [PC].[CriticalSectionTokens]
		SET [DateGranted] = GETDATE()
			,[DateReturned] = NULL
			,[Status] = 0
			,[ExecutionId] = @ExecutionId
		WHERE [TaskId] = @TaskId
	
		SELECT @ProcessId AS [TaskId]
				,@ExecutionId AS [ExecutionId]
				,1 AS [Status]
		
	END
	ELSE
	BEGIN
	
		SELECT 0 AS [TaskId]
				,'00000000-0000-0000-0000-000000000000' AS [ExecutionId]
				,0 AS [Status];
	
	END";

        public const string ReturnCriticalSectionTokenQuery = @"UPDATE [PC].[CriticalSectionTokens] 
	SET [DateReturned] = GETDATE()
		,[Status] = 1
	WHERE [ProcessId] = @TaskId
	AND [ExecutionId] = @ExecutionId; ";

        
    }
}
