CREATE TABLE [Taskling].[TaskExecutionEvent](
	[TaskExecutionEventId] [bigint] IDENTITY(1,1) NOT NULL,
	[TaskExecutionId] [int] NOT NULL,
	[EventType] [tinyint] NOT NULL,
	[Message] [nvarchar](MAX) NULL,
	[EventDateTime] [datetime] NOT NULL,
 CONSTRAINT [PK_TaskExecutionEvent] PRIMARY KEY CLUSTERED 
(
	[TaskExecutionEventId] ASC
)
)
