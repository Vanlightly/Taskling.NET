CREATE TABLE [Taskling].[TaskDefinition](
	[TaskDefinitionId] [int] IDENTITY(1,1) NOT NULL,
	[ApplicationName] [varchar](200) NOT NULL,
	[TaskName] [varchar](200) NOT NULL,
	[LastCleaned] [datetime] NULL,
	[ExecutionTokens] [varchar](max) NULL,
	[UserCsStatus] [tinyint] NOT NULL,
	[UserCsTaskExecutionId] [int] NULL,
	[UserCsQueue] [varchar](max) NULL,
	[ClientCsStatus] [tinyint] NOT NULL,
	[ClientCsTaskExecutionId] [int] NULL,
	[ClientCsQueue] [varchar](max) NULL,
	[HoldLockTaskExecutionId] [int] NULL,
    CONSTRAINT [PK_TaskDefinition] PRIMARY KEY ([TaskDefinitionId]) 
)