CREATE TABLE [Taskling].[ListBlockItem](
	[ListBlockItemId] [bigint] IDENTITY(1,1) NOT NULL,
	[BlockId] [bigint] NOT NULL,
	[Value] [varchar](200) NOT NULL,
	[Status] [tinyint] NOT NULL,
	[BlockExecutionId] [bigint] NULL,
 CONSTRAINT [PK_ListBlockItem] PRIMARY KEY CLUSTERED 
(
	[BlockId] ASC,
	[ListBlockItemId] ASC
)
)
