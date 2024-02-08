-- returns the dominant ecogroup polygons with correct percentages from the area
-- replace 'my_table' with actual table to clip by and 'group_cols' with group columns from clip_table.

-- with group columns
WITH clip_table AS (
SELECT group_cols, geom
  FROM my_table
 WHERE my_col = 'something'
    
), clip AS (
SELECT r.groupcols, q.areasymbol, q.spatialver, q.musym, q.mukey,
       CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
            ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
  FROM soil.mupolygon AS q
 INNER JOIN clip_table AS r ON ST_Intersects(q.geom, r.geom)
    
), area_calc AS (
SELECT group_cols, areasymbol, spatialver, musym, mukey, geom,
      ST_Area(geography(geom))/10000 AS area_ha
  FROM clip
    
), area_pct AS (
SELECT a.group_cols, COALESCE(b.ecogroup, 'NA') AS ecogroup, b.groupname, b.grouptype, b.pub_status, c.veg_type,
       a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha, a.geom
  FROM area_calc AS a
  LEFT JOIN soil.ecogroup_mudominant AS b ON a.mukey = b.mukey
    LEFT JOIN soil.ecogroup_meta AS c ON b.ecogroup = c.ecogroup

), area_union AS (
SELECT group_cols, ST_Union(geom) AS geom, ecogroup, Min(groupname) AS groupname,  Min(grouptype) AS grouptype, 
       min(pub_status) AS pub_status, min(veg_type) veg_type, sum(ecogrouparea_ha) AS ecogrouparea_ha
  FROM area_pct
 GROUP BY group_cols, ecogroup

), area_calc_union AS (
SELECT group_cols, ecogroup, groupname, grouptype, pub_status, veg_type, ST_Area(geography(geom))/10000 AS area_ha, ecogrouparea_ha,
      ST_Multi(geom) AS geom
  FROM area_union

-- CW for ESRI CCW otherwise
), make_valid AS (
SELECT group_cols, ecogroup, groupname, grouptype, pub_status, veg_type, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, ST_ForcePolygonCW(ST_MakeValid(geom)) AS geom
  FROM area_calc_union

--production section
--creates a comma delimited list of plants in an ecogroup in descending production order
), forest_agg AS (
SELECT ecogroup, plantsym, substring(prodtype from 'forest|range') prodtype, sum(prod) prod 
  FROM soil.ecogroup_plantprod
 GROUP BY ecogroup, plantsym, prodtype

), prod_agg AS (
SELECT ecogroup, string_agg(plantsym, ',' ORDER BY prod desc) AS production, prodtype
  FROM forest_agg
 GROUP BY ecogroup, prodtype
    
), prod AS (
SELECT a.ecogroup, b.production range_prod, c.production AS forest_prod
  FROM (SELECT ecogroup FROM soil.ecogroup_plantprod GROUP BY ecogroup) AS a
  LEFT JOIN (SELECT * FROM prod_agg WHERE prodtype LIKE 'range%') AS b ON a.ecogroup = b.ecogroup
  LEFT JOIN (SELECT * FROM prod_agg WHERE prodtype LIKE 'forest%') AS c ON a.ecogroup = c.ecogroup
-- /end production section


--must be sure to have ecolass_unique loaded (aka ecoclass)
), group_mlra AS (
SELECT ecoclassid_std AS ecogroup, mlra 
  FROM soil.ecoclass WHERE ecoclassid_std IS NOT NULL
 GROUP BY ecoclassid_std, mlra
UNION
SELECT ecogroup, split_part(ecogroup, ' ', 1) AS mlra 
  FROM soil.ecogroup_meta

), mlra_prod AS (
SELECT group_cols, a.ecogroup, a.groupname, a.grouptype, a.pub_status, a.veg_type, a.area_ha, a.ecogrouppct, 
       b.range_prod, b.forest_prod, c.mlra, a.geom 
  FROM make_valid AS a
  LEFT JOIN prod AS b ON a.ecogroup = b.ecogroup
  LEFT JOIN group_mlra AS c ON a.ecogroup = c.ecogroup
)

SELECT group_cols, ecogroup, groupname, grouptype, pub_status, range_prod, forest_prod, veg_type,
       mlra, area_ha, ecogrouppct, geom 
  FROM mlra_prod ORDER BY group_cols, area_ha DESC;



-- 
-- no group columns
--
WITH clip_table AS (
SELECT geom
  FROM my_table
 WHERE my_col = 'something'
    
), clip AS (
SELECT q.areasymbol, q.spatialver, q.musym, q.mukey,
       CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
            ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
  FROM soil.mupolygon AS q
 INNER JOIN clip_table AS r ON ST_Intersects(q.geom, r.geom)
    
), area_calc AS (
SELECT areasymbol, spatialver, musym, mukey, geom,
      ST_Area(geography(geom))/10000 AS area_ha
  FROM clip
    
), area_pct AS (
SELECT COALESCE(b.ecogroup, 'NA') AS ecogroup, b.groupname, b.grouptype, b.pub_status, c.veg_type,
       a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha, a.geom
  FROM area_calc AS a
  LEFT JOIN soil.ecogroup_mudominant AS b ON a.mukey = b.mukey
    LEFT JOIN soil.ecogroup_meta AS c ON b.ecogroup = c.ecogroup

), area_union AS (
SELECT ST_Union(geom) AS geom, ecogroup, Min(groupname) AS groupname,  Min(grouptype) AS grouptype, 
       min(pub_status) AS pub_status, min(veg_type) veg_type, sum(ecogrouparea_ha) AS ecogrouparea_ha
  FROM area_pct
 GROUP BY ecogroup

), area_calc_union AS (
SELECT ecogroup, groupname, grouptype, pub_status, veg_type, ST_Area(geography(geom))/10000 AS area_ha, ecogrouparea_ha,
      ST_Multi(geom) AS geom
  FROM area_union

-- CW for ESRI CCW otherwise
), make_valid AS (
SELECT ecogroup, groupname, grouptype, pub_status, veg_type, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, ST_ForcePolygonCW(ST_MakeValid(geom)) AS geom
  FROM area_calc_union

--production section
--creates a comma delimited list of plants in an ecogroup in descending production order
), forest_agg AS (
SELECT ecogroup, plantsym, substring(prodtype from 'forest|range') prodtype, sum(prod) prod 
  FROM soil.ecogroup_plantprod
 GROUP BY ecogroup, plantsym, prodtype

), prod_agg AS (
SELECT ecogroup, string_agg(plantsym, ',' ORDER BY prod desc) AS production, prodtype
  FROM forest_agg
 GROUP BY ecogroup, prodtype
    
), prod AS (
SELECT a.ecogroup, b.production range_prod, c.production AS forest_prod
  FROM (SELECT ecogroup FROM soil.ecogroup_plantprod GROUP BY ecogroup) AS a
  LEFT JOIN (SELECT * FROM prod_agg WHERE prodtype LIKE 'range%') AS b ON a.ecogroup = b.ecogroup
  LEFT JOIN (SELECT * FROM prod_agg WHERE prodtype LIKE 'forest%') AS c ON a.ecogroup = c.ecogroup
-- /end production section


--must be sure to have ecolass_unique loaded (aka ecoclass)
), group_mlra AS (
SELECT ecoclassid_std AS ecogroup, mlra 
  FROM soil.ecoclass WHERE ecoclassid_std IS NOT NULL
 GROUP BY ecoclassid_std, mlra
UNION
SELECT ecogroup, split_part(ecogroup, ' ', 1) AS mlra 
  FROM soil.ecogroup_meta

), mlra_prod AS (
SELECT a.ecogroup, a.groupname, a.grouptype, a.pub_status, a.veg_type, a.area_ha, a.ecogrouppct, 
       b.range_prod, b.forest_prod, c.mlra, a.geom 
  FROM make_valid AS a
  LEFT JOIN prod AS b ON a.ecogroup = b.ecogroup
  LEFT JOIN group_mlra AS c ON a.ecogroup = c.ecogroup
)

SELECT ecogroup, groupname, grouptype, pub_status, range_prod, forest_prod, veg_type,
       mlra, area_ha, ecogrouppct, geom 
  FROM mlra_prod ORDER BY area_ha DESC;