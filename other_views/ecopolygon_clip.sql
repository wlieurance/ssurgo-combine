-- returns the dominant ecosite polygons with correct percentages from the area
-- replace 'my_table' with actual table to clip by
WITH clip_table AS (
SELECT group_cols, geom
  FROM my_table
 WHERE my_col = 'something'

), clip AS (
SELECT q.areasymbol, q.spatialver, q.musym, q.mukey,
       r.groupcols,
       CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
            ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
 FROM soil.mupolygon AS q
INNER JOIN clip_table AS r ON ST_Intersects(q.geom, r.geom)

), area_calc AS (
SELECT group_cols, areasymbol, spatialver, musym, mukey, geom,
       ST_Area(geography(geom))/10000 AS area_ha
  FROM clip

), area_pct AS (
SELECT a.group_cols, COALESCE(b.ecoclassid_std, 'NA') AS ecoclassid_std, b.ecoclassname, b.ecotype, 
       a.area_ha*(CAST(b.ecoclasspct AS NUMERIC)/100) AS ecoarea_ha, a.geom
  FROM area_calc AS a
  LEFT JOIN soil.coecoclass_mudominant AS b ON a.mukey = b.mukey

), area_union AS (
SELECT group_cols, ST_Union(geom) AS geom, ecoclassid_std, min(ecoclassname) AS ecoclassname, min(ecotype) AS ecotype, sum(ecoarea_ha) AS ecoarea_ha
  FROM area_pct
 GROUP BY group_cols, ecoclassid_std
 
), area_calc_union AS (
SELECT group_cols, ecoclassid_std, ecoclassname, ecotype, ecoarea_ha, ST_Area(geography(geom))/10000 AS area_ha,
       ST_Multi(geom) AS geom
  FROM area_union

), make_valid AS (
SELECT group_cols, ecoclassid_std, ecoclassname, ecotype, area_ha, ecoarea_ha/area_ha AS ecopct, ST_ForceRHR(ST_MakeValid(geom)) AS geom
  FROM area_calc_union

--production section
--creates a comma delimited list of plants in an ecogroup in descending production order
), forest_agg AS (
SELECT ecoclassid_std, plantsym, substring(prodtype from 'forest|range') prodtype, sum(prod) prod 
  FROM soil.coecoclass_plantprod
 GROUP BY ecoclassid_std, plantsym, prodtype

), prod_agg AS (
SELECT ecoclassid_std, string_agg(plantsym, ',' ORDER BY prod DESC) AS production, prodtype
  FROM forest_agg
 GROUP BY ecoclassid_std, prodtype
    
), prod AS (
SELECT a.ecoclassid_std, b.production range_prod, c.production forest_prod
  FROM (SELECT ecoclassid_std FROM soil.coecoclass_plantprod GROUP BY ecoclassid_std) AS a
  LEFT JOIN (SELECT * FROM prod_agg WHERE prodtype LIKE 'range%') AS b ON a.ecoclassid_std = b.ecoclassid_std
  LEFT JOIN (SELECT * FROM prod_agg WHERE prodtype LIKE 'forest%') AS c ON a.ecoclassid_std = c.ecoclassid_std
-- /end production section

), group_mlra AS (
SELECT ecoclassid_std, min(mlra) mlra 
  FROM soil.ecoclass WHERE ecoclassid_std IS NOT NULL
 GROUP BY ecoclassid_std

), mlra_prod AS (
SELECT a.group_cols, a.ecoclassid_std, a.ecoclassname, a.ecotype, a.area_ha, a.ecopct, b.range_prod, b.forest_prod,
       d.habitat veg_type, c.mlra, a.geom 
  FROM make_valid AS a
  LEFT JOIN prod AS b ON a.ecoclassid_std = b.ecoclassid_std
  LEFT JOIN group_mlra AS c ON a.ecoclassid_std = c.ecoclassid_std
  LEFT JOIN soil.coecoclass_habitat d ON a.ecoclassid_std = d.ecoclassid_std
)

SELECT group_cols, ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct, range_prod, forest_prod,
       veg_type, mlra, geom
  FROM mlra_prod ORDER BY group_cols, area_ha DESC;