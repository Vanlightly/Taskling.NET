CREATE TABLE [Taskling].[CriticalSectionToken](
	[TaskDefinitionId] [int] NOT NULL,
	[DateGranted] [datetime] NULL,
	[DateReturned] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[TaskExecutionId] [int] NULL,
	[HoldLockTaskExecutionId] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[TaskDefinitionId] ASC
)
)
