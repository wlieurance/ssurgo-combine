-- returns a list of map units, their assocaited ecosites and the theoretical area of each ecosite in that area in hectares.
-- replace 'my_table' with actual table to clip by
WITH geom_table AS (
SELECT * FROM my_table

), mupolygon_clip AS (
SELECT x.areasymbol, x.spatialver, x.musym, x.mukey,
	   CASE WHEN ST_CoveredBy(x.geom, y.geom) then x.geom
		    ELSE ST_Multi(ST_Intersection(x.geom, y.geom)) END AS geom
  FROM soil.mupolygon AS x
 INNER JOIN geom_table AS y ON ST_Intersects(x.geom, y.geom)

), calc_area AS (
SELECT areasymbol, spatialver, musym, mukey,
	   ST_Area(Geography(geom))/10000 AS area_ha, geom
 FROM mupolygon_clip

), join_eco AS (
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha, b.ecoclassid, 
       b.ecoclassid_std, b.ecoclassname, 
       b.ecoclassname_std, b.ecotype, b.ecoclasspct, b.ecorank,
       a.area_ha * b.ecoclasspct::double precision / 100::double precision AS ecosite_ha
  FROM calc_area a
  LEFT JOIN soil.coecoclass_mapunit_ranked b ON a.mukey::text = b.mukey::text
)

SELECT * FROM join_eco ORDER BY areasymbol, musym, ecorank;
