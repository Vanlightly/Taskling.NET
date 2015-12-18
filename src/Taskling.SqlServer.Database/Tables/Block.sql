CREATE TABLE [Taskling].[Block](
	[BlockId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskDefinitionId] [int] NOT NULL,
	[FromDate] [datetime] NULL,
	[ToDate] [datetime] NULL,
	[FromNumber] [bigint] NULL,
	[ToNumber] [bigint] NULL,
	[BlockType] [tinyint] NOT NULL,
	[CreatedDate] [datetime] NOT NULL DEFAULT GETUTCDATE(),
 CONSTRAINT [PK_Block] PRIMARY KEY CLUSTERED 
(
	[BlockId] ASC
)
)