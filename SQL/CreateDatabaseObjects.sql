/****** Object:  Schema [orchestration] ******/
CREATE SCHEMA [orchestration]
GO
/****** Object:  Schema [staging] ******/
CREATE SCHEMA [staging]
GO
/****** Object:  Table [orchestration].[ProcessingControl] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [orchestration].[ProcessingControl](
	[TableId] [int] IDENTITY(1,1) NOT NULL,
	[SourceSchema] [nvarchar](128) NOT NULL,
	[SourceTable] [nvarchar](128) NOT NULL,
	[TargetSchema] [nvarchar](128) NOT NULL,
	[TargetTable] [nvarchar](128) NOT NULL,
	[KeyColumnName] [nvarchar](255) NULL,
	[IsIncremental] [bit] NOT NULL,
 CONSTRAINT [PK_ProcessingControl] PRIMARY KEY CLUSTERED 
(
	[TableId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [orchestration].[ProcessingLog] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [orchestration].[ProcessingLog](
	[TableId] [int] NOT NULL,
	[PipelineRunId] [varchar](50) NOT NULL,
	[ProcessingStarted] [datetime2](7) NOT NULL,
	[ProcessingEnded] [datetime2](7) NULL,
	[LowWatermark] [datetime2](7) NULL,
	[HighWatermark] [datetime2](7) NULL,
 CONSTRAINT [PK_ProcessingLog_1] PRIMARY KEY CLUSTERED 
(
	[TableId] ASC,
	[PipelineRunId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_ProcessingControl_TableNames] ******/
CREATE UNIQUE NONCLUSTERED INDEX [IX_ProcessingControl_TableNames] ON [orchestration].[ProcessingControl]
(
	[SourceSchema] ASC,
	[SourceTable] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [orchestration].[GetTablesToProcess] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [orchestration].[GetTablesToProcess]

AS

SELECT PC.TableId,
	PC.SourceSchema,
	PC.SourceTable,
	PC.TargetSchema,
	PC.TargetTable,
	PC.KeyColumnName,
	ISNULL(PC.IsIncremental, 0) AS IsIncremental,
	ISNULL(MAX(PL.HighWatermark), '1900-01-01') AS LowWatermark,
	GETUTCDATE() AS HighWatermark
FROM orchestration.ProcessingControl PC
	LEFT OUTER JOIN orchestration.ProcessingLog PL
		ON PC.TableId= PL.TableId
		AND PL.ProcessingEnded IS NOT NULL
GROUP BY PC.TableId,
	PC.SourceSchema,
	PC.SourceTable,
	PC.TargetSchema,
	PC.TargetTable,
	PC.KeyColumnName,
	PC.IsIncremental
GO
