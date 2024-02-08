-- returns the map unit polygons 
-- replace 'my_table' with actual table to clip by

-- without group columns
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

), area_union AS (
SELECT ST_Union(geom) AS geom, areasymbol, spatialver, musym, mukey   
  FROM clip
 GROUP BY areasymbol, spatialver, musym, mukey

), area_calc_union AS (
SELECT areasymbol, spatialver, musym, mukey, ST_Area(geography(geom))/10000 AS area_ha,
      ST_Multi(geom) AS geom
  FROM area_union

), make_valid AS (
SELECT areasymbol, spatialver, musym, mukey, ST_ForcePolygonCW(ST_MakeValid(geom)) AS geom
  FROM area_calc_union
)

SELECT b.lkey, a.areasymbol, c.areaname, a.mukey, a.musym, a.spatialver, b.muname, b.mukind, a.geom
  FROM make_valid a
  LEFT JOIN soil.mapunit b ON a.mukey = b.mukey
  LEFT JOIN soil.legend c ON b.lkey = c.lkey;