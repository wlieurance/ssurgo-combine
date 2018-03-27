spatialite_tables = {'date':'REALDATE', 'text':'TEXT', 'limit_text':'TEXT', 'int':'INTEGER', 'short_int':'INT16', 'long_int':'INT32', 
                    'float':'FLOAT64', 'double':'FLOAT64', 'blob':'BLOB', 'guid':'UUIDTEXT', 'geom': 'GEOMETRYBLOB', 
                    'oid':'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL', 'bool':'INTEGER'}
postgis_tables    = {'date':'TIMESTAMP', 'text':'TEXT', 'limit_text':'VARCHAR', 'int':'INTEGER', 'short_int':'SMALLINT', 'long_int':'INTEGER', 
                    'float':'NUMERIC(6,6)', 'double':'NUMERIC(15,7)', 'blob':'BYTEA', 'guid':'VARCHAR(38)', 'geom':'GEOMETRY', 'raster':'BYTEA',
                    'oid':'SERIAL PRIMARY KEY NOT NULL', 'bool':'BOOLEAN'}
