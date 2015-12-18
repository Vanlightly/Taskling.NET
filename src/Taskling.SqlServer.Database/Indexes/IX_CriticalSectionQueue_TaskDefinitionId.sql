CREATE NONCLUSTERED INDEX [IX_CriticalSectionQueue_TaskDefinitionId] ON [Taskling].[CriticalSectionQueue] 
(
	[TaskDefinitionId] ASC,
	[TaskExecutionId] ASC
)