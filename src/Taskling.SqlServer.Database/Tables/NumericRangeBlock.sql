CREATE TABLE [Taskling].[NumericRangeBlock](
	[NumericRangeBlockId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[FromNumber] [bigint] NOT NULL,
	[ToNumber] [bigint] NOT NULL,
	[CreatedDate] [datetime] NOT NULL DEFAULT GETUTCDATE(),
 CONSTRAINT [PK_NumericRangeBlock] PRIMARY KEY CLUSTERED 
(
	[NumericRangeBlockId] ASC
)
)
