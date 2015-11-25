CREATE TABLE [Taskling].[TaskExecution](
	[TaskExecutionId] [int] IDENTITY(1,1) NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
[LastKeepAlive] DATETIME NOT NULL, 
    PRIMARY KEY CLUSTERED 
(
	[TaskExecutionId] ASC
))
