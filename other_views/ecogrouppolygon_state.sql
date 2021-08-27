DROP TABLE IF EXISTS soil.ecogrouppolygon_state;

CREATE TABLE soil.ecogrouppolygon_state AS
WITH state_table AS ( 
-- source from US Census Bureau State spatial features
SELECT stusps, geom FROM {schema}.state

), subgroup AS (
SELECT * 
  FROM soil.ecogroup_mapunit_ranked 
 WHERE grouprank = 1
	
), group_area_mu AS (
SELECT ST_Intersection(a.geom, c.geom) AS geom, COALESCE(b.ecogroup, 'NA') AS ecogroup, b.groupname, b.grouptype, b.pub_status,
	   a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha, c.stusps AS state
  FROM soil.mupolygon AS a
  LEFT JOIN subgroup AS b ON a.mukey = b.mukey
  INNER JOIN state_table c ON ST_Intersects(a.geom, c.geom)

), group_union AS (
SELECT state, ST_Multi(ST_Union(geom)) AS geom, ecogroup, min(groupname) AS groupname, min(grouptype) AS grouptype, 
	   min(pub_status) AS pub_status, sum(ecogrouparea_ha) AS ecogrouparea_ha
  FROM group_area_mu
 GROUP BY state, ecogroup

), group_cleaned AS (
SELECT state, ecogroup, groupname, grouptype, pub_status, ecogrouparea_ha,  
       ST_ForcePolygonCCW(ST_CollectionExtract(ST_MakeValid(geom), 3)) AS geom
  FROM group_union

), group_area AS (
SELECT state, ecogroup, groupname, grouptype, pub_status, ST_Area(geography(geom))/10000 AS area_ha, ecogrouparea_ha,
	   geom
  FROM group_cleaned
 WHERE ST_IsValid(geom)

), group_pct AS (
SELECT state, ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, geom
  FROM group_area
)

SELECT * FROM  group_pct;

CREATE INDEX ecogrouppolygon_state_geom_idx
  ON ecogrouppolygon_state
  USING GIST (geom);