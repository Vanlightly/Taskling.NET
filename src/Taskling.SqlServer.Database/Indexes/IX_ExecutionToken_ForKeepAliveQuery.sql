CREATE NONCLUSTERED INDEX [IX_ExecutionToken_ForKeepAliveQuery] ON [Taskling].[ExecutionToken] 
(
	[TaskSecondaryId] ASC,
	[Status] ASC,
	[LastKeepAlive] ASC
)