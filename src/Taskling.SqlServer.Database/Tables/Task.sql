CREATE TABLE [Taskling].[Task](
	[ApplicationName] [varchar](200) NOT NULL,
	[TaskName] [varchar](200) NOT NULL,
	[TaskSecondaryId] [int] IDENTITY(1,1) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[ApplicationName] ASC,
	[TaskName] ASC
))
