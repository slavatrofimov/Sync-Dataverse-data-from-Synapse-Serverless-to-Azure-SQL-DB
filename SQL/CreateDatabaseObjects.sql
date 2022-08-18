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
	GETUTCDATE() AS HighWatermark,
	CAST(CASE WHEN PC.IsIncremental = 1 AND PK.CONSTRAINT_NAME IS NULL THEN 1 ELSE 0 END AS BIT) AS NeedsPrimaryKey
FROM orchestration.ProcessingControl PC
	LEFT OUTER JOIN orchestration.ProcessingLog PL
		ON PC.TableId= PL.TableId
		AND PL.ProcessingEnded IS NOT NULL
	LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
		ON PC.TargetSchema = PK.TABLE_SCHEMA
		AND PC.TargetTable = PK.TABLE_NAME
		AND PK.CONSTRAINT_TYPE = 'Primary Key'
GROUP BY PC.TableId,
	PC.SourceSchema,
	PC.SourceTable,
	PC.TargetSchema,
	PC.TargetTable,
	PC.KeyColumnName,
	PC.IsIncremental,
	PK.CONSTRAINT_NAME
GO
/****** Object:  StoredProcedure [orchestration].[GeneratePrimaryKey] ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [orchestration].[GeneratePrimaryKey]
	@Schema nvarchar(128), 
	@Table nvarchar(128), 
	@KeyColumn nvarchar(128)

AS

DECLARE @SQL nvarchar(MAX)

--Make sure that a Primary Key constraint does not exist on the specified table
IF NOT EXISTS (
	SELECT * 
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
	WHERE CONSTRAINT_TYPE = 'Primary Key'
	AND TABLE_SCHEMA = @Schema
	AND TABLE_NAME = @Table
)

BEGIN
	--Make key column non-nullable
	SELECT @SQL = 'ALTER TABLE ['+ @Schema + '].[' + @Table + '] ALTER COLUMN [' + @KeyColumn + '] ' + system_type_name + ' NOT NULL'
	FROM sys.dm_exec_describe_first_result_set('SELECT [' + @KeyColumn + '] FROM  ['+ @Schema + '].[' + @Table + ']' , NULL, NULL)
	EXEC sp_executesql @stmt = @SQL

	--Add primary key
	SET @SQL = 'ALTER TABLE ['+ @Schema + '].[' + @Table + '] ADD CONSTRAINT [PK_' + @Schema + '_' + @Table + '] PRIMARY KEY CLUSTERED ([' + @KeyColumn + '])'
	EXEC sp_executesql @stmt = @SQL
END
GO
