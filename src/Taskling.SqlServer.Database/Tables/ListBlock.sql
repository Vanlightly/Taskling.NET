CREATE TABLE [Taskling].[ListBlock](
	[ListBlockId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[CreatedDate] [datetime] NOT NULL DEFAULT GETUTCDATE(),
 CONSTRAINT [PK_ListBlock] PRIMARY KEY CLUSTERED 
(
	[ListBlockId] ASC
)
)
