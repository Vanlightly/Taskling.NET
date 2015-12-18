CREATE TABLE [Taskling].[TaskExecution](
	[TaskExecutionId] [int] IDENTITY(1,1) NOT NULL,
	[TaskDefinitionId] [int] NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
	[LastKeepAlive] [datetime] NULL, 
	[ServerName] [varchar](200) NOT NULL,
	[TaskDeathMode] [tinyint] NOT NULL,
    [OverrideThreshold] [time] NULL,
    [KeepAliveInterval] [time] NULL,
    [KeepAliveDeathThreshold] [time] NULL,
    PRIMARY KEY CLUSTERED 
(
	[TaskExecutionId] ASC
))
