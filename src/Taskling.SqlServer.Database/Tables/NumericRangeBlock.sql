CREATE TABLE [Taskling].[NumericRangeBlock](
	[NumericRangeBlockId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskSecondaryId] [int] NOT NULL,
	[FromNumber] [bigint] NOT NULL,
	[ToNumber] [bigint] NOT NULL,
 CONSTRAINT [PK_NumericRangeBlock] PRIMARY KEY CLUSTERED 
(
	[NumericRangeBlockId] ASC
)
)
