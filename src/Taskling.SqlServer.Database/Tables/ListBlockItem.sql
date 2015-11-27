CREATE TABLE [Taskling].[ListBlockItem](
	[ListBlockItemId] [bigint] IDENTITY(1,1) NOT NULL,
	[ListBlockId] [bigint] NOT NULL,
	[Value] [varchar](200) NOT NULL,
	[Status] [tinyint] NOT NULL,
 CONSTRAINT [PK_ListBlockItem] PRIMARY KEY CLUSTERED 
(
	[ListBlockId] ASC,
	[ListBlockItemId] ASC
)
)
