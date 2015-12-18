CREATE TABLE [Taskling].[BlockExecution](
	[BlockExecutionId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskExecutionId] [int] NOT NULL,
	[BlockId] [bigint] NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
	[BlockExecutionStatus] [tinyint] NOT NULL,
 CONSTRAINT [PK_BlockExecution] PRIMARY KEY CLUSTERED 
(
	[BlockExecutionId] ASC
)
)