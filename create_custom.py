custom_spatialite = [
"""SELECT DiscardGeometryColumn('ecopolygon', 'geom');""",

"""DROP TABLE IF EXISTS {schema}.ecopolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecosite polygons. */
CREATE TABLE IF NOT EXISTS {schema}.ecopolygon (
       ecoclassid_std {limit_text} (20) PRIMARY KEY,
       ecoclassname {text},
       ecotype {limit_text} (20),
       area_ha {double},
       ecopct {double});""",

"""SELECT AddGeometryColumn('ecopolygon', 'geom', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecosite per polygon with area percentage of ecosite. Inserted into table for usefulness and speed. */
INSERT INTO {schema}.ecopolygon (ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct, geom)
SELECT ecoclassid_std, ecoclassname, ecotype, area_ha, ecoarea_ha/area_ha AS ecopct, geom
  FROM (
       SELECT ecoclassid_std, ecoclassname, ecotype, ST_Area(geom, 1)/10000 AS area_ha, ecoarea_ha, ST_Multi(geom) AS geom
         FROM (
              SELECT ST_Union(geom) AS geom, ecoclassid_std, ecoclassname, ecotype, sum(ecoarea_ha) AS ecoarea_ha
                FROM (
                     SELECT a.geom, COALESCE(b.ecoclassid_std, 'NA') AS ecoclassid_std, b.ecoclassname, b.ecotype, 
                            a.area_ha*(CAST(b.ecoclasspct AS REAL)/100) AS ecoarea_ha
                       FROM {schema}.mupolygon AS a
                       LEFT JOIN {schema}.coecoclass_mudominant AS b ON a.mukey = b.mukey)
                     AS x
               GROUP BY ecoclassid_std) AS y)
       AS z;"""
]

custom_postgis = [

"""DROP TABLE IF EXISTS {schema}.ecopolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecosite polygons. */
CREATE TABLE IF NOT EXISTS {schema}.ecopolygon (
       ecoclassid_std {limit_text} (20) PRIMARY KEY,
       ecoclassname {text},
       ecotype {limit_text} (20),
       area_ha {double},
       ecopct {double},
       geom geometry('MULTIPOLYGON', 4326));""",

#"""SELECT AddGeometryColumn('{schema}.ecopolygon', 'geom', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecosite per polygon with area percentage of ecosite. Inserted into table for usefulness and speed. */
INSERT INTO {schema}.ecopolygon (ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct, geom)
SELECT ecoclassid_std, ecoclassname, ecotype, area_ha, ecoarea_ha/area_ha AS ecopct, ST_ForceRHR(ST_MakeValid(geom)) AS geom
  FROM (
       SELECT ecoclassid_std, ecoclassname, ecotype, ecoarea_ha, ST_Area(geography(geom))/10000 AS area_ha,
              ST_Multi(geom) AS geom
         FROM (
              SELECT ST_Union(geom) AS geom, ecoclassid_std, min(ecoclassname) AS ecoclassname, min(ecotype) AS ecotype, sum(ecoarea_ha) AS ecoarea_ha
                FROM (
                     SELECT a.geom, COALESCE(b.ecoclassid_std, 'NA') AS ecoclassid_std, b.ecoclassname, b.ecotype, 
                            a.area_ha*(CAST(b.ecoclasspct AS NUMERIC)/100) AS ecoarea_ha
                       FROM {schema}.mupolygon AS a
                       LEFT JOIN {schema}.coecoclass_mudominant AS b ON a.mukey = b.mukey)
                     AS x
               GROUP BY ecoclassid_std) AS y)
       AS z;"""
]
