CREATE TABLE [Taskling].[DateRangeBlockExecution](
	[DateRangeBlockExecutionId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskExecutionId] [int] NOT NULL,
	[DateRangeBlockId] [bigint] NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
	[BlockExecutionStatus] [tinyint] NOT NULL,
 CONSTRAINT [PK_DateRangeBlockExecution] PRIMARY KEY CLUSTERED 
(
	[DateRangeBlockExecutionId] ASC
)
)
