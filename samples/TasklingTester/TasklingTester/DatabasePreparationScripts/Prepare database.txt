-- first create your database called MyAppDb then run this script

CREATE TABLE Journey
(
	JourneyId int identity(1,1) not null primary key,
	DepartureStation varchar(50) not null,
	ArrivalStation varchar(50) not null,
	TravelDate datetime not null,
	PassengerName varchar(100) not null
)

CREATE TABLE TravelInsight
(
	TravelInsightId int identity(1,1) not null primary key,
	PassengerName varchar(100) not null,
	InsightText varchar(1000) not null,
	InsightDate datetime not null
)

DECLARE @BaseDate DateTime = '20000101'
DECLARE @Current int = (SELECT COALESCE(MAX(JourneyId), 0) FROM [MyAppDb].[dbo].[Journey]) + 1

WHILE @Current <= 10000
BEGIN
	INSERT INTO [MyAppDb].[dbo].[Journey]
           ([DepartureStation]
           ,[ArrivalStation]
           ,[TravelDate]
           ,[PassengerName])
     VALUES
           ('DEP' + CAST(@Current AS VARCHAR(5))
           ,'ARR' + CAST(@Current AS VARCHAR(5))
           ,DATEADD(MINUTE, @Current, @BaseDate)
           ,'Passenger' + CAST(@Current AS VARCHAR(5)))
	

	SET @Current = @Current + 1
END


-------------------------------
-- Create Taskling Tables
-------------------------------

SET XACT_ABORT ON
GO
BEGIN TRAN
GO

CREATE SCHEMA [Taskling]
GO

CREATE TABLE [Taskling].[Block](
    [BlockId] [bigint] IDENTITY(1,1) NOT NULL,
    [TaskDefinitionId] [int] NOT NULL,
    [FromDate] [datetime] NULL,
    [ToDate] [datetime] NULL,
    [FromNumber] [bigint] NULL,
    [ToNumber] [bigint] NULL,
    [ObjectData] [nvarchar](MAX) NULL,
    [CompressedObjectData] [varbinary](MAX) NULL,
    [BlockType] [tinyint] NOT NULL,
    [IsPhantom] [bit] NOT NULL DEFAULT 0,
    [CreatedDate] [datetime] NOT NULL DEFAULT GETUTCDATE(),
 CONSTRAINT [PK_Block] PRIMARY KEY CLUSTERED 
(
    [BlockId] ASC
))

CREATE TABLE [Taskling].[BlockExecution](
    [BlockExecutionId] [bigint] IDENTITY(1,1) NOT NULL,
    [TaskExecutionId] [int] NOT NULL,
    [BlockId] [bigint] NOT NULL,
    [StartedAt] [datetime] NULL,
    [CompletedAt] [datetime] NULL,
    [CreatedAt] [datetime] NOT NULL,
    [Attempt] [int] NOT NULL,
    [ItemsCount] [int] NULL,
    [BlockExecutionStatus] [tinyint] NOT NULL,
 CONSTRAINT [PK_BlockExecution] PRIMARY KEY CLUSTERED 
(
    [BlockExecutionId] ASC
))

CREATE TABLE [Taskling].[ListBlockItem](
    [ListBlockItemId] [bigint] IDENTITY(1,1) NOT NULL,
    [BlockId] [bigint] NOT NULL,
    [Value] [nvarchar](MAX) NULL,
    [CompressedValue] [varbinary](MAX) NULL,
    [Status] [tinyint] NOT NULL,
    [Timestamp] DATETIME NULL, 
    [LastUpdated] DATETIME NULL, 
    [StatusReason][nvarchar](MAX) NULL,
    [Step][tinyint] NULL,
    CONSTRAINT [PK_ListBlockItem] PRIMARY KEY CLUSTERED 
(
    [BlockId] ASC,
    [ListBlockItemId] ASC
))

CREATE TABLE [Taskling].[TaskDefinition](
    [TaskDefinitionId] [int] IDENTITY(1,1) NOT NULL,
    [ApplicationName] [varchar](200) NOT NULL,
    [TaskName] [varchar](200) NOT NULL,
    [LastCleaned] [datetime] NULL,
    [ExecutionTokens] [varchar](max) NULL,
    [UserCsStatus] [tinyint] NOT NULL,
    [UserCsTaskExecutionId] [int] NULL,
    [UserCsQueue] [varchar](max) NULL,
    [ClientCsStatus] [tinyint] NOT NULL,
    [ClientCsTaskExecutionId] [int] NULL,
    [ClientCsQueue] [varchar](max) NULL,
    [HoldLockTaskExecutionId] [int] NULL,
    CONSTRAINT [PK_TaskDefinition] PRIMARY KEY ([TaskDefinitionId]) 
)

CREATE TABLE [Taskling].[TaskExecution](
    [TaskExecutionId] [int] IDENTITY(1,1) NOT NULL,
    [TaskDefinitionId] [int] NOT NULL,
    [StartedAt] [datetime] NOT NULL,
    [CompletedAt] [datetime] NULL,
    [LastKeepAlive] [datetime] NOT NULL,
    [ServerName] [varchar](200) NOT NULL,
    [TaskDeathMode] [tinyint] NOT NULL,
    [OverrideThreshold] [time](7) NULL,
    [KeepAliveInterval] [time](7) NULL,
    [KeepAliveDeathThreshold] [time](7) NULL,
    [FailedTaskRetryLimit] [smallint] NOT NULL,
    [DeadTaskRetryLimit] [smallint] NOT NULL,
    [ReferenceValue] [nvarchar](200) NULL,
    [Failed] [bit] NOT NULL,
    [Blocked] [bit] NOT NULL,
    [TasklingVersion] VARCHAR(50) NULL, 
    [ExecutionHeader] [nvarchar](MAX) NULL,
    PRIMARY KEY CLUSTERED 
(
    [TaskExecutionId] ASC
))

CREATE TABLE [Taskling].[TaskExecutionEvent](
    [TaskExecutionEventId] [bigint] IDENTITY(1,1) NOT NULL,
    [TaskExecutionId] [int] NOT NULL,
    [EventType] [tinyint] NOT NULL,
    [Message] [nvarchar](MAX) NULL,
    [EventDateTime] [datetime] NOT NULL,
 CONSTRAINT [PK_TaskExecutionEvent] PRIMARY KEY CLUSTERED 
(
    [TaskExecutionEventId] ASC
)) 
GO

CREATE TABLE [Taskling].[ForceBlockQueue](
    [ForceBlockQueueId] [int] IDENTITY(1,1) NOT NULL,
    [BlockId] [bigint] NOT NULL,
    [ForcedDate] [datetime] NOT NULL,
    [ForcedBy] [varchar](50) NOT NULL,
    [ProcessingStatus] [varchar](20) NOT NULL,
 CONSTRAINT [PK_ForceBlockQueue] PRIMARY KEY CLUSTERED 
(
    [ForceBlockQueueId] ASC
)) 
GO


ALTER TABLE [Taskling].[ForceBlockQueue] ADD  DEFAULT (getutcdate()) FOR [ForcedDate]
GO

ALTER TABLE [Taskling].[ForceBlockQueue] ADD  DEFAULT ('Pending') FOR [ProcessingStatus]
GO

CREATE NONCLUSTERED INDEX [IX_Block_TaskDefinitionId] ON [Taskling].[Block] 
(
    [TaskDefinitionId] ASC
)

CREATE NONCLUSTERED INDEX [IX_BlockExecution_BlockId] ON [Taskling].[BlockExecution] 
(
    [BlockId] ASC
)

CREATE NONCLUSTERED INDEX [IX_BlockExecution_TaskExecutionId] ON [Taskling].[BlockExecution] 
(
    [TaskExecutionId] ASC
)

CREATE UNIQUE NONCLUSTERED INDEX [IX_TaskDefinition_Unique] ON [Taskling].[TaskDefinition] 
(
    [ApplicationName] ASC,
    [TaskName] ASC
)


CREATE NONCLUSTERED INDEX [IX_TaskExecution_TaskDefinitionId] ON [Taskling].[TaskExecution] 
(
    [TaskDefinitionId] ASC
)


COMMIT TRAN
GO

SET XACT_ABORT OFF
GO