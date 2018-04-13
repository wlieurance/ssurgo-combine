spatialite_tables = {'date':'REALDATE', 'text':'TEXT', 'limit_text':'TEXT', 'int':'INTEGER', 'short_int':'INT16', 'long_int':'INT32', 
                    'float':'FLOAT64', 'double':'FLOAT64', 'blob':'BLOB', 'guid':'UUIDTEXT', 'geom': 'GEOMETRYBLOB', 
                    'oid':'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL', 'bool':'INTEGER NOT NULL', 'notexists':'IF NOT EXISTS'}
postgis_tables    = {'date':'TIMESTAMP', 'text':'TEXT', 'limit_text':'VARCHAR', 'int':'INTEGER', 'short_int':'SMALLINT', 'long_int':'INTEGER', 
                    'float':'NUMERIC(6,6)', 'double':'NUMERIC(15,7)', 'blob':'BYTEA', 'guid':'VARCHAR(38)', 'geom':'GEOMETRY', 'raster':'BYTEA',
                    'oid':'SERIAL PRIMARY KEY NOT NULL', 'bool':'BOOLEAN', 'notexists':'IF NOT EXISTS'}
mssql_tables      = {'date':'DATETIME2(7)', 'text':'NVARCHAR', 'limit_text':'NVARCHAR', 'int':'INT', 'short_int':'SMALLINT', 'long_int':'INT', 
                    'float':'NUMERIC(6,6)', 'double':'NUMERIC(15,7)', 'blob':'VARBINARY', 'guid':'UNIQUEIDENTIFIER', 'geom':'GEOMETRY', 'raster':'INT',
                    'oid':'INT PRIMARY KEY NOT NULL', 'bool':'BIT NOT NULL', 'notexists':''}
