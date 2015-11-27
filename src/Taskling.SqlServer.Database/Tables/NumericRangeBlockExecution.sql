CREATE TABLE [Taskling].[NumericRangeBlockExecution](
	[NumericRangeBlockExecutionId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskExecutionId] [int] NOT NULL,
	[NumericRangeBlockId] BIGINT NOT NULL,
	[StartedAt] [datetime] NOT NULL,
	[CompletedAt] [datetime] NULL,
	[BlockExecutionStatus] [tinyint] NOT NULL DEFAULT 0,
 CONSTRAINT [PK_NumericRangeBlockExecution] PRIMARY KEY CLUSTERED 
(
	[NumericRangeBlockExecutionId] ASC
)
)
