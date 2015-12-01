CREATE TABLE [Taskling].[ExecutionToken](
	[ExecutionTokenId] INT IDENTITY NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[DateGranted] [datetime] NULL,
	[DateReturned] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[TaskExecutionId] [int] NULL,
	[HoldLockTaskExecutionId] [int] NULL,
	[LastKeepAlive] [datetime] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[ExecutionTokenId] ASC
))