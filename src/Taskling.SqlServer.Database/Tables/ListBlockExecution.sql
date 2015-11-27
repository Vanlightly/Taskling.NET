CREATE TABLE [Taskling].[ListBlockExecution](
	[ListBlockExecutionId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskExecutionId] [int] NOT NULL,
	[ListBlockId] [bigint] NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
	[BlockExecutionStatus] [tinyint] NOT NULL,
 CONSTRAINT [PK_ListBlockExecution] PRIMARY KEY CLUSTERED 
(
	[ListBlockExecutionId] ASC
)
)
