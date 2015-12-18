CREATE TABLE [Taskling].[CriticalSectionQueue](
	[CriticalSectionQueueIndex] [int] IDENTITY(1,1) NOT NULL,
	[TaskDefinitionId] [int] NOT NULL,
	[TaskExecutionId] [int] NULL,
	[HoldLockTaskExecutionId] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[TaskDefinitionId] ASC,
	[CriticalSectionQueueIndex] ASC
)
)
