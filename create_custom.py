custom_spatialite = [
"""SELECT DiscardGeometryColumn('ecopolygon', 'shape');""",

"""DROP TABLE IF EXISTS ecopolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecosite polygons. */
CREATE TABLE IF NOT EXISTS ecopolygon (
       OBJECTID {oid}, 
       ecoclassid_std {limit_text} (20),
       ecoclassname {text},
       ecotype {limit_text} (20),
       area_dd {double},
       ecopct {double});""",

"""SELECT AddGeometryColumn('ecopolygon', 'shape', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecosite per polygon with area percentage of ecosite. Inserted into table for usefulness and speed. */
INSERT INTO ecopolygon (ecoclassid_std, ecoclassname, ecotype, area_dd, ecopct, shape)
SELECT ecoclassid_std, ecoclassname, ecotype, ST_Area(shape) AS area_dd, (ecoarea_dd/ST_Area(shape)) AS ecopct, ST_Multi(shape) AS shape
  FROM (
       SELECT ST_Union(shape) AS shape, ecoclassid_std, ecoclassname, ecotype, sum(ecoarea_dd) AS ecoarea_dd
         FROM (
              SELECT a.shape, b.ecoclassid_std, b.ecoclassname, b.ecotype, 
                     (ST_Area(a.shape)*(CAST(b.ecoclasspct AS REAL)/100)) AS ecoarea_dd
                FROM mupolygon AS a
                LEFT JOIN coecoclass_mudominant AS b ON a.mukey = b.mukey)
              AS x
        GROUP BY ecoclassid_std) AS y;"""
]

custom_postgis = [

"""DROP TABLE IF EXISTS ecopolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecosite polygons. */
CREATE TABLE IF NOT EXISTS ecopolygon (
       OBJECTID {oid}, 
       ecoclassid_std {limit_text} (20),
       ecoclassname {text},
       ecotype {limit_text} (20),
       area_dd {double},
       ecopct {double});""",

"""SELECT AddGeometryColumn('ecopolygon', 'shape', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecosite per polygon with area percentage of ecosite. Inserted into table for usefulness and speed. */
INSERT INTO ecopolygon (ecoclassid_std, ecoclassname, ecotype, area_dd, ecopct, shape)
SELECT ecoclassid_std, ecoclassname, ecotype, ST_Area(shape) AS area_dd, (ecoarea_dd/ST_Area(shape)) AS ecopct, ST_Multi(shape) AS shape
  FROM (
       SELECT ST_Union(shape) AS shape, ecoclassid_std, min(ecoclassname) AS ecoclassname, min(ecotype) AS ecotype, sum(ecoarea_dd) AS ecoarea_dd
         FROM (
              SELECT a.shape, b.ecoclassid_std, b.ecoclassname, b.ecotype, 
                     (ST_Area(a.shape)*(CAST(b.ecoclasspct AS NUMERIC)/100)) AS ecoarea_dd
                FROM mupolygon AS a
                LEFT JOIN coecoclass_mudominant AS b ON a.mukey = b.mukey)
              AS x
        GROUP BY ecoclassid_std) AS y;"""
]
