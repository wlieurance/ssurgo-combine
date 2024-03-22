WITH clip_table AS (
SELECT geom
  FROM my_table
 WHERE my_value = 'some value'
    
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
SELECT areasymbol, spatialver, musym, mukey,
      ST_Multi(geom) AS geom
  FROM area_union

), make_valid AS (
SELECT areasymbol, spatialver, musym, mukey, ST_ForcePolygonCW(ST_MakeValid(geom)) AS geom
  FROM area_calc_union

), mupolygon_clip AS (
SELECT b.lkey, a.areasymbol, c.areaname, a.mukey, a.musym, a.spatialver, b.muname, b.mukind, 
	   ST_Area(geography(a.geom))/10000 AS area_ha, a.geom
  FROM make_valid a
  LEFT JOIN soil.mapunit b ON a.mukey = b.mukey
  LEFT JOIN soil.legend c ON b.lkey = c.lkey

), component_clipped AS (
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha,
       b.ecoclassid, b.ecoclassid_std, b.ecoclassname, b.ecoclassname_std,
       b.ecotype, b.ecoclasspct, b.ecorank,
       a.area_ha * b.ecoclasspct::double precision / 100::double precision AS eco_ha
  FROM mupolygon_clip a
  LEFT JOIN soil.coecoclass_mapunit_ranked b ON a.mukey = b.mukey
)

SELECT ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, ecotype, sum(eco_ha) AS eco_ha
  FROM component_clipped
 WHERE eco_ha IS NOT NULL
 GROUP BY ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, ecotype
 ORDER BY ecoclassid_std;
