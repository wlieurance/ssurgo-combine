CREATE TABLE soil.ecopolygon_state AS
WITH state_table AS ( 
-- source from US Census Bureau State spatial features
SELECT stusps, geom FROM {schema}.state
	
), subgroup AS (
SELECT * 
  FROM {schema}.coecoclass_mapunit_ranked 
 WHERE ecorank = 1
	
), eco_area_mu AS (
SELECT ST_Intersection(a.geom, c.geom) AS geom, COALESCE(b.ecoclassid_std, 'NA') AS ecoclassid_std, b.ecoclassname, b.ecotype, 
	   a.area_ha*(CAST(b.ecoclasspct AS NUMERIC)/100) AS ecoarea_ha, c.stusps AS state
  FROM {schema}.mupolygon AS a
  LEFT JOIN subgroup AS b ON a.mukey = b.mukey
  INNER JOIN state_table c ON ST_Intersects(a.geom, c.geom)

), eco_union AS (
SELECT state, ST_Multi(ST_Union(geom)) AS geom, ecoclassid_std, min(ecoclassname) AS ecoclassname, min(ecotype) AS ecotype, sum(ecoarea_ha) AS ecoarea_ha
  FROM eco_area_mu
 GROUP BY state, ecoclassid_std

), eco_cleaned AS (
SELECT state, ecoclassid_std, ecoclassname, ecotype, ecoarea_ha,
       ST_ForcePolygonCCW(ST_CollectionExtract(ST_MakeValid(geom), 3)) AS geom
  FROM eco_union

), eco_area AS (
SELECT state, ecoclassid_std, ecoclassname, ecotype, ecoarea_ha, ST_Area(geography(geom))/10000 AS area_ha, geom
  FROM eco_cleaned
 WHERE ST_IsValid(geom)

), eco_pct AS (
SELECT state, ecoclassid_std, ecoclassname, ecotype, area_ha, ecoarea_ha/area_ha AS ecopct, geom
  FROM eco_area
)

SELECT * FROM eco_pct;
