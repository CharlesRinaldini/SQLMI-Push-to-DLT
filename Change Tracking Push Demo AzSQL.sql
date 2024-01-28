CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'a123STRONGpassword!';

CREATE DATABASE SCOPED CREDENTIAL SQL_Credential
    WITH IDENTITY = 'crossDBUser',
    SECRET = 'a123STRONGpassword!';

DROP EXTERNAL DATA SOURCE RemoteSqlMI

CREATE EXTERNAL DATA SOURCE RemoteSqlMI
WITH (
    TYPE = RDBMS,
    LOCATION = '<yourSQLMIName>.public.<abc123def456>.database.windows.net,<port>',
    DATABASE_NAME = 'demo',
    CREDENTIAL = SQL_Credential
);

create table dbo.changeTrackingLog (
	ctLogId int identity(1, 1) primary key,
	srcTableName varchar(100),
	ctVersion bigint,
	startDateTime datetime,
	endDateTime datetime
)
go

create or alter procedure dbo.startCTLoad
	@tableName varchar(100),
	@ctVersion bigint
as 
begin
	insert into dbo.changeTrackingLog (srcTableName, ctVersion, startDateTime)
	select @tableName, @ctVersion, getutcdate() 

	select @@IDENTITY as ctLogId
end
go

create or alter procedure dbo.updateCTLoad
	@ctLogId bigint
as
begin
	update dbo.changeTrackingLog
	set endDateTime = getutcdate()
	where ctLogId = @ctLogId
end
go

create or alter procedure dbo.exportExternalDatabaseTableToParquet
	@tableName varchar(100),
	@fullLoad int = 0
as
begin
	
	declare @currentWatermarkId int
	declare @previousWatermarkId int
	declare @sqlCmd varchar(1000)
	declare @extSql nvarchar(100)
	declare @ctLoadId int

	begin try  		
		select @previousWatermarkId = coalesce((select top 1 ctVersion
			from dbo.changeTrackingLog
			where srcTableName = @tableName
			and endDateTime is not null
			order by startDateTime desc), 0)

		create table #watermark (WatermarkID int, extraInfo varchar(1000))

		insert into #watermark
		exec sp_execute_remote  
			N'RemoteSqlMI',  
			N'SELECT Watermark = CHANGE_TRACKING_CURRENT_VERSION()'  

		set @currentWatermarkId = (select WatermarkID from #watermark)
		
		create table #ctLoad (ctLogId int)

		insert #ctLoad
		exec dbo.startCTLoad @tableName, @currentWatermarkId

		select @ctLoadId = ctLogId from #ctLoad

		if @fullLoad = -1
		begin
			set @extSql = 'exec dbo.exportSqlToParquet ''' + @tableName + ''', 0'  
		end
		else
		begin
			set @extSql = 'exec dbo.exportSqlToParquet ''' + @tableName + ''', ' + cast(@previousWatermarkId as varchar(100))
		end

		print @extSql

		create table #remoteOutput (partitionName varchar(1000), extraInfo varchar(1000))

		insert #remoteOutput
		exec sp_execute_remote  
		N'RemoteSqlMI',
		@extSql

		exec sp_invoke_external_rest_endpoint @url = 'https://<yourAzureFunctionApp>.azurewebsites.net/api/<functionName>',
			@method = 'POST'

		exec dbo.updateCTLoad @ctLoadId
		
		select 'created and processed partition : ' + partitionName as outputMsg from #remoteOutput

	end try  
	begin catch  
		 SELECT ERROR_NUMBER() AS ErrorNumber  
		,ERROR_SEVERITY() AS ErrorSeverity  
		,ERROR_STATE() AS ErrorState  
		,ERROR_PROCEDURE() AS ErrorProcedure  
		,ERROR_LINE() AS ErrorLine  
		,ERROR_MESSAGE() AS ErrorMessage; 
		 --Log error
	end catch 
end