DECLARE @sql NVARCHAR(MAX);

SET @sql = N'SELECT 1';

EXEC sp_executesql @sql;
