/* Creates a new table in which store spatial query results for dominant ecosite polygons. */
DROP TABLE IF EXISTS {schema}.ecopolygon;
CREATE TABLE IF NOT EXISTS {schema}.ecopolygon (
       ecoclassid_std VARCHAR (20) PRIMARY KEY,
       ecoclassname TEXT,
       ecotype VARCHAR (20),
       area_ha DOUBLE PRECISION,
       ecopct DOUBLE PRECISION);
       
ALTER TABLE {schema}.ecopolygon ADD COLUMN IF NOT EXISTS geom geometry('MULTIPOLYGON', 4326);



/* Spatial view showing dominant ecosite per polygon with area percentage of ecosite. Inserted into table for usefulness and speed. 
{st_direction}: OGC sf = ST_ForcePolygonCCW, ESRI = ST_ForcePolygonCW */
INSERT INTO {schema}.ecopolygon (ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct, geom)
WITH subgroup AS (
SELECT * 
  FROM {schema}.coecoclass_mapunit_ranked 
 WHERE ecorank = 1

), eco_area_mu AS (
SELECT a.geom, COALESCE(b.ecoclassid_std, 'NA' || '_' || a.areasymbol) AS ecoclassid_std, b.ecoclassname, b.ecotype, 
       a.area_ha*(CAST(b.ecoclasspct AS REAL)/100) AS ecoarea_ha
  FROM {schema}.mupolygon AS a
  LEFT JOIN subgroup AS b ON a.mukey = b.mukey

), eco_union AS (
SELECT ST_Multi(ST_Union(geom)) AS geom, ecoclassid_std, min(ecoclassname) AS ecoclassname, min(ecotype) AS ecotype, sum(ecoarea_ha) AS ecoarea_ha
  FROM eco_area_mu
 GROUP BY ecoclassid_std

), eco_cleaned AS (
SELECT ecoclassid_std, ecoclassname, ecotype, ecoarea_ha,
       {st_direction}(ST_CollectionExtract(ST_MakeValid(geom), 3)) AS geom
  FROM eco_union

), eco_area AS (
SELECT ecoclassid_std, ecoclassname, ecotype, ecoarea_ha, ST_Area(geom, True)/10000 AS area_ha, geom
  FROM eco_cleaned
 WHERE ST_IsValid(geom)

), eco_pct AS (
SELECT ecoclassid_std, ecoclassname, ecotype, area_ha, ecoarea_ha/area_ha AS ecopct, ST_Multi(geom) geom
  FROM eco_area
)

SELECT * FROM eco_pct;

CREATE INDEX IF NOT EXISTS ecopolygon_gix ON {schema}.ecopolygon USING GIST (geom);
