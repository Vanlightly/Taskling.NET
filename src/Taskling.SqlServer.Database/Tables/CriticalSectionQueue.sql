CREATE TABLE [Taskling].[CriticalSectionQueue](
	[CriticalSectionQueueIndex] [int] IDENTITY(1,1) NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[TaskExecutionId] [int] NULL,
	[HoldLockTaskExecutionId] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[CriticalSectionQueueIndex] ASC,
	[TaskSecondaryId] ASC
)
)
