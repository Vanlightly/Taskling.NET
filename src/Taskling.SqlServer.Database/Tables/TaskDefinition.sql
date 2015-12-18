CREATE TABLE [Taskling].[TaskDefinition](
	[TaskDefinitionId] [int] IDENTITY(1,1) NOT NULL,
	[ApplicationName] [varchar](200) NOT NULL,
	[TaskName] [varchar](200) NOT NULL
PRIMARY KEY CLUSTERED 
(
	[TaskDefinitionId] ASC
))
