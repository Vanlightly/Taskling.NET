CREATE TABLE [Taskling].[ExecutionToken](
	[ExecutionTokenId] INT IDENTITY NOT NULL,
	[TaskDefinitionId] [int] NOT NULL,
	[DateGranted] [datetime] NULL,
	[DateReturned] [datetime] NULL,
	[Status] [tinyint] NOT NULL,
	[TaskExecutionId] [int] NULL,
	[LastKeepAlive] [datetime] NULL, 
	[HoldLockTaskExecutionId] [int] NULL
PRIMARY KEY CLUSTERED 
(
	[TaskDefinitionId] ASC,
	[ExecutionTokenId] ASC
))