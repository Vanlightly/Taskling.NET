CREATE TABLE [Taskling].[DateRangeBlock](
	[DateRangeBlockId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[FromDate] [datetime] NOT NULL,
	[ToDate] [datetime] NOT NULL,
	[CreatedDate] [datetime] NOT NULL DEFAULT GETUTCDATE(),
 CONSTRAINT [PK_DateRangeBlock] PRIMARY KEY CLUSTERED 
(
	[DateRangeBlockId] ASC
)
)
