CREATE UNIQUE NONCLUSTERED INDEX [IX_TaskDefinition_Unique] ON [Taskling].[TaskDefinition] 
(
	[ApplicationName] ASC,
	[TaskName] ASC
)