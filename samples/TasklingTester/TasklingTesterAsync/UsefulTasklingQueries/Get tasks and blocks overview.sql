SELECT TOP 1000 T.TaskName
		,TE.TaskExecutionId
		,TE.StartedAt AS TaskStarted
		,TE.CompletedAt AS TaskCompleted
		,TE.LastKeepAlive
		,TE.Failed AS TaskFailed
		,TE.Blocked AS TaskBlocked
		,CASE WHEN B.BlockType IS NULL THEN 'No Blocks'
			WHEN B.BlockType = 1 THEN 'NumericRange'
			WHEN B.BlockType = 2 THEN 'DateRange'
			WHEN B.BlockType = 3 THEN 'List'
			WHEN B.BlockType = 3 THEN 'Object' ELSE 'Not Defined' END AS BlockType
		,BE.BlockExecutionId
		,B.BlockId
		,CASE WHEN B.BlockType IS NULL THEN '-'
			WHEN B.BlockType = 1 THEN CAST(B.FromNumber AS VARCHAR(50))
			WHEN B.BlockType = 2 THEN CONVERT(VARCHAR(20), B.FromDate, 20)
			WHEN B.BlockType = 3 THEN '-'
			WHEN B.BlockType = 4 THEN '-' ELSE 'X' END AS BlockFrom
		,CASE WHEN B.BlockType IS NULL THEN '-'
			WHEN B.BlockType = 1 THEN CAST(B.ToNumber AS VARCHAR(50))
			WHEN B.BlockType = 2 THEN CONVERT(VARCHAR(20), B.ToDate, 20)
			WHEN B.BlockType = 3 THEN '-'
			WHEN B.BlockType = 4 THEN '-' ELSE 'X' END AS BlockTo
		,CASE WHEN BE.BlockExecutionStatus IS NULL THEN '-'
			WHEN BE.BlockExecutionStatus = 1 THEN 'Not Started'
			WHEN BE.BlockExecutionStatus = 2 THEN 'Started'
			WHEN BE.BlockExecutionStatus = 3 THEN 'Completed'
			WHEN BE.BlockExecutionStatus = 4 THEN 'Failed' END AS BlockExecutionStatus
		,CASE B.BlockType
			WHEN 1 THEN CAST(BE.ItemsCount AS VARCHAR(50))
			WHEN 2 THEN CAST(BE.ItemsCount AS VARCHAR(50))
			WHEN 3 THEN ListBlockStats
			ELSE '-' END AS ItemsProcessed
		,BE.Attempt
		,BE.StartedAt AS BlockStarted
		,BE.CompletedAt AS BlockCompleted
		,BE.CreatedAt AS BlockCreated
FROM [Taskling].TaskExecution TE WITH(NOLOCK) 
LEFT JOIN [Taskling].TaskDefinition T WITH(NOLOCK) ON T.TaskDefinitionId = TE.TaskDefinitionId
LEFT JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON TE.TaskExecutionId = BE.TaskExecutionId
LEFT JOIN [Taskling].Block B WITH(NOLOCK) ON BE.BlockId = B.BlockId
OUTER APPLY 
(
	SELECT 'Items-> ' + STUFF((
		SELECT ',' 
			+ CAST(
			CASE [Status] WHEN 1 THEN 'Pending'
				WHEN 2 THEN 'Completed'
				WHEN 3 THEN 'Failed'
				WHEN 4 THEN 'Discarded' ELSE 'X' END
			AS VARCHAR(50))
			+ ':' 
			+ CAST(Freq AS VARCHAR(50))
		FROM (
			SELECT BlockId
				  ,[Status]
				  ,COUNT(*) AS Freq
			FROM [Taskling].[ListBlockItem] WITH(NOLOCK)
			WHERE BlockId = B.BlockId
			GROUP BY BlockId, [Status]
		) t
		FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS ListBlockStats
) _ 
ORDER BY TE.StartedAt desc