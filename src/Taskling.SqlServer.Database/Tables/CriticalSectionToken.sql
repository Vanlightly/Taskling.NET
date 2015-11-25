CREATE TABLE [Taskling].[CriticalSectionToken](
	[TaskSecondaryId] [int] NOT NULL,
	[DateGranted] [datetime] NULL,
	[DateReturned] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[TaskExecutionId] [int] NULL,
	[HoldLockTaskExecutionId] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[TaskSecondaryId] ASC
)
)
