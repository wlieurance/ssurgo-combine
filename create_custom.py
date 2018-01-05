custom_statements = [
"""DROP TABLE IF EXISTS ecopolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecosite polygons. */
CREATE TABLE IF NOT EXISTS ecopolygon (
       OBJECTID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
       ecoclassid_std TEXT (20),
       ecoclassname TEXT,
       ecotype TEXT (20),
       area_ha FLOAT64,
       ecopct FLOAT64);""",

"""SELECT AddGeometryColumn('ecopolygon', 'SHAPE', 4326, 'MULTIPOLYGON', 'XY');""",

"""/* Spatial view showing dominant ecosite per polygon with area percentage of ecosite. Inserted into table for usefulness and speed. */
INSERT INTO ecopolygon (ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct, SHAPE)
SELECT ecoclassid_std, ecoclassname, ecotype, (ST_Area(SHAPE, 1)/10000) AS area_ha, (ecoarea_sqm/ST_Area(SHAPE, 1)) AS ecopct, ST_Multi(SHAPE) AS SHAPE
  FROM (
       SELECT ST_Union(SHAPE) AS SHAPE, ecoclassid_std, ecoclassname, ecotype, sum(ecoarea_sqm) AS ecoarea_sqm
         FROM (
              SELECT a.SHAPE, b.ecoclassid_std, b.ecoclassname, b.ecotype, 
                     (ST_Area(a.SHAPE, 1)*(CAST(b.ecoclasspct AS REAL)/100)) AS ecoarea_sqm
                FROM mupolygon AS a
                LEFT JOIN coecoclass_mudominant AS b ON a.mukey = b.mukey)
              AS x
        GROUP BY ecoclassid_std) AS y;"""
]
