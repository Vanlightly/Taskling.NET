CREATE TABLE [Taskling].[TaskExecution](
	[TaskExecutionId] [int] IDENTITY(1,1) NOT NULL,
	[TaskDefinitionId] [int] NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
	[LastKeepAlive] [datetime] NOT NULL,
	[ServerName] [varchar](200) NOT NULL,
	[TaskDeathMode] [tinyint] NOT NULL,
	[OverrideThreshold] [time](7) NULL,
	[KeepAliveInterval] [time](7) NULL,
	[KeepAliveDeathThreshold] [time](7) NULL,
	[FailedTaskRetryLimit] [smallint] NOT NULL,
	[DeadTaskRetryLimit] [smallint] NOT NULL,
	[ReferenceValue] [nvarchar](200) NULL,
	[Failed] [bit] NOT NULL,
	[Blocked] [bit] NOT NULL,
    [TasklingVersion] VARCHAR(50) NULL, 
    PRIMARY KEY CLUSTERED 
(
	[TaskExecutionId] ASC
))