-- returns the dominant ecogroup polygons with correct percentages from the area
-- replace 'my_table' with actual table to clip by
WITH clip AS (
SELECT q.areasymbol, q.spatialver, q.musym, q.mukey,
       CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
            ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
  FROM soil.mupolygon AS q
 INNER JOIN (SELECT * FROM my_table WHERE my_condition) AS r ON ST_Intersects(q.geom, r.geom)
    
), area_calc AS (
SELECT areasymbol, spatialver, musym, mukey, geom,
      ST_Area(geography(geom))/10000 AS area_ha
  FROM clip
    
), area_pct AS (
SELECT a.geom, COALESCE(b.ecogroup, 'NA') AS ecogroup, b.groupname, b.grouptype, b.pub_status,
       a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha
  FROM area_calc AS a
  LEFT JOIN soil.ecogroup_mudominant AS b ON a.mukey = b.mukey

), area_union AS (
SELECT ST_Union(geom) AS geom, ecogroup, Min(groupname) AS groupname,  Min(grouptype) AS grouptype, 
       min(pub_status) AS pub_status, sum(ecogrouparea_ha) AS ecogrouparea_ha
  FROM area_pct
 GROUP BY ecogroup

), area_calc_union AS (
SELECT ecogroup, groupname, grouptype, pub_status, ST_Area(geography(geom))/10000 AS area_ha, ecogrouparea_ha,
      ST_Multi(geom) AS geom
  FROM area_union
    
), make_valid AS (
SELECT ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, ST_ForceRHR(ST_MakeValid(geom)) AS geom
  FROM area_calc_union
)

SELECT * FROM make_valid