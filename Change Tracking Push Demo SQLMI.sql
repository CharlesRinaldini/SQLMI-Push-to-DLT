ALTER DATABASE demo  
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON)  

create table dbo.demoTable (
	demoId int identity(1, 1) primary key, 
	field1 varchar(40),
	desc1 varchar(100)
)

ALTER TABLE dbo.demoTable 
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON)  

insert into dbo.demoTable (field1, desc1)
select 'abc123', 'initial insert'
union all
select 'def234', 'initial insert'
union all
select 'ghi345', 'initial insert'
union all
select 'jkl456', 'initial insert'
union all
select 'mno567', 'initial insert'
union all
select 'pqr678', 'initial insert'
union all
select 'stu789', 'initial insert'
union all
select 'vwx890', 'initial insert'
go

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'a123STRONGpassword!';

CREATE DATABASE SCOPED CREDENTIAL [WorkspaceIdentity] WITH IDENTITY = 'managed identity';
GO

CREATE EXTERNAL FILE FORMAT [ParquetFF] WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

CREATE EXTERNAL DATA SOURCE [SQLwriteable] WITH (
    LOCATION = 'adls://bronze@<yourStorageAccount>.blob.core.windows.net/',
    CREDENTIAL = [WorkspaceIdentity]
);
GO

create or alter procedure dbo.exportSqlToParquet
	@srcTable varchar(1000),
	@ctVersion bigint
as
begin
	declare @sqlCmd varchar(1000)
	declare @colSelect varchar(1000)
	declare @colJoin varchar(1000)
	declare @folderHNS varchar(100)
	declare @schemaName varchar(100)
	declare @tableName varchar(100)
	declare @extTableName varchar(100)
	
	select @schemaName = substring(@srcTable, 0, charindex('.', @srcTable)), @tableName = substring(@srcTable, charindex('.', @srcTable) + 1, 100)
	set @extTableName = @tableName + '_Ext'

	begin try
		exec('if (select 1
			from sys.external_tables as e
			join sys.schemas as s
			on e.schema_id = s.schema_id
			where s.name = ''' + @schemaName + '''
			and e.name = ''' + @extTableName + ''') is not null
			drop external table ' + @schemaName + '.' + @extTableName)

		set @folderHNS = (select 'yyyy=' + cast(datepart(yyyy, getutcdate()) as varchar(4)) + 
								'/MM=' + right('00' + cast(datepart(MM, getutcdate()) as varchar(2)), 2) + 
								'/dd=' +  right('00' + cast(datepart(dd, getutcdate()) as varchar(2)), 2) + '/')

		set @colSelect = (select string_agg(case c.is_identity when 1 then 'ct.' else 'x.' end + c.name, ', ') as colSelect
			from sys.columns as c
			join sys.tables as t
			on c.object_id = t.object_id
			join sys.schemas as s
			on t.schema_id = s.schema_id
			where s.name = @schemaName
			and t.name = @tableName)

		set @colJoin = (select string_agg('ct.' + c.name + ' = x.' + c.name, ' and ') as colJoin
			from sys.columns as c
			join sys.tables as t
			on c.object_id = t.object_id
			join sys.schemas as s
			on t.schema_id = s.schema_id
			where s.name = @schemaName
			and t.name = @tableName
			and c.is_identity = 1)

		if @ctVersion <= 0
		begin
			set @sqlCmd = 'CREATE EXTERNAL TABLE ' + @srcTable + '_Ext WITH (
				LOCATION = ''entity=' + @tableName + '/' + @folderHNS + ''',
				DATA_SOURCE = [SQLwriteable],
				FILE_FORMAT = [ParquetFF]
			) AS
			select *, GETUTCDATE() as changeTimeUTC, ''I'' as changeOperation from ' + @srcTable
		end
		else if @ctVersion > 0
		begin
			set @sqlCmd = 'CREATE EXTERNAL TABLE ' + @srcTable + '_Ext WITH (
				LOCATION = ''entity=' + @tableName + '/' + @folderHNS + ''',
				DATA_SOURCE = [SQLwriteable],
				FILE_FORMAT = [ParquetFF]
			) AS
			SELECT ' + @colSelect + ', GETUTCDATE() as changeTimeUTC, ct.SYS_CHANGE_OPERATION as changeOperation 
			FROM CHANGETABLE(CHANGES ' + @srcTable + ', ' + cast(@ctVersion as varchar(100)) + ') as ct 
			LEFT JOIN ' + @srcTable + ' x (NOLOCK) 
			ON ' + @colJoin 
		end
	
		exec(@sqlCmd)

		exec('drop external table ' + @schemaName + '.' + @extTableName)

		select 'entity=' + @tableName + '/' + @folderHNS  as outputMsg
	end try
	begin catch
		exec('if (select 1
			from sys.external_tables as e
			join sys.schemas as s
			on e.schema_id = s.schema_id
			where s.name = ''' + @schemaName + '''
			and e.name = ''' + @extTableName + ''') is not null
			drop external table ' + @schemaName + '.' + @extTableName)
		select ERROR_MESSAGE() AS outputMsg
	end catch

end
go