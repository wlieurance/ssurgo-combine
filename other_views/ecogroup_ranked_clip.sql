-- clips mupolygon by a table and returns the sum of the ecogroups and ecogroup area in the clipped result.
-- replace 'my_table' with actual table to clip by and change/remove 'group_cols' throughout in order to keep track within certain clip groups
WITH clip_table AS (
SELECT group_cols, geom
  FROM my_table
 WHERE some_col IN = 'something'
 
), clip AS (
SELECT x.areasymbol, x.spatialver, x.musym, x.mukey,
	   y.group_cols
	 CASE WHEN ST_CoveredBy(x.geom, y.geom) then x.geom
		  ELSE ST_Multi(ST_Intersection(x.geom, y.geom)) END AS geom
FROM soil.mupolygon AS x
INNER JOIN clip_table AS y ON ST_Intersects(x.geom, y.geom)

), area_calc AS (
SELECT areasymbol, spatialver, musym, mukey, 
       group_cols
	   ST_Area(Geography(geom))/10000 AS area_ha, geom
 FROM isect

), area_pct AS (
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha, b.ecogroup,
	   a.group_cols
	   b.grouptype, b.pub_status, b.ecogrouppct, b.grouprank, b.groupname,
	   a.area_ha * b.ecogrouppct::double precision / 100::double precision AS ecogroup_ha
  FROM area_calc AS a
  LEFT JOIN soil.ecogroup_mapunit_ranked b ON a.mukey::text = b.mukey::text

), area_union AS (
SELECT group_cols, ecogroup, grouptype, max(groupname) AS groupname, sum(ecogroup_ha) AS ecogroup_ha
  FROM area_pct
 GROUP BY group_cols, ecogroup, grouptype, pub_status
)

SELECT *, row_number() over(partition by group_cols order by ecogroup_ha DESC) AS group_rank
  FROM area_union ORDER BY group_cols, ecogroup_ha DESC
