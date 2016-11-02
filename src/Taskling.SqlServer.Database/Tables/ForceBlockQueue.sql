CREATE TABLE [Taskling].[ForceBlockQueue](
	[ForceBlockQueueId] [int] IDENTITY(1,1) NOT NULL,
	[BlockId] [bigint] NOT NULL,
	[ForcedDate] [datetime] NOT NULL,
	[ForcedBy] [varchar](50) NOT NULL,
	[ProcessingStatus] [varchar](20) NOT NULL,
 CONSTRAINT [PK_ForceBlockQueue] PRIMARY KEY CLUSTERED 
(
	[ForceBlockQueueId] ASC
)
)
