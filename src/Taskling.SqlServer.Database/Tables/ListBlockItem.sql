CREATE TABLE [Taskling].[ListBlockItem](
	[ListBlockItemId] [bigint] IDENTITY(1,1) NOT NULL,
	[BlockId] [bigint] NOT NULL,
	[Value] [nvarchar](max) NULL,
	[CompressedValue] [varbinary](MAX) NULL,
	[Status] [tinyint] NOT NULL,
    [Timestamp] DATETIME NULL, 
	[LastUpdated] DATETIME NULL, 
	[StatusReason] nvarchar(max) NULL,
	[Step] tinyint NULL,
    CONSTRAINT [PK_ListBlockItem] PRIMARY KEY CLUSTERED 
(
	[BlockId] ASC,
	[ListBlockItemId] ASC
)
)
