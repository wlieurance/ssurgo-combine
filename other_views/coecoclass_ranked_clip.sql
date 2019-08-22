-- returns a list of ecosites and the theoretical area of each ecosite in hectares.
-- replace 'my_table' with actual table to clip by
SELECT max(ecoclassid) AS ecoclassid, ecoclassid_std, max(ecoclassname) AS ecoclassname, 
       max(ecoclassname_std) AS ecoclassname_std, max(ecotype) AS ecotype, SUM(ecosite_ha) AS ecosite_ha
  FROM (
	SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha, b.ecoclassid, b.ecoclassid_std, b.ecoclassname, 
		   b.ecoclassname_std, b.ecotype, b.ecoclasspct, b.ecorank,
		   a.area_ha * b.ecoclasspct::double precision / 100::double precision AS ecosite_ha
	  FROM (
		   SELECT z.areasymbol, z.spatialver, z.musym, z.mukey,
				   ST_Area(Geography(z.geom))/10000 AS area_ha, z.geom
			 FROM (
				  SELECT x.areasymbol, x.spatialver, x.musym, x.mukey,
						 CASE WHEN ST_CoveredBy(x.geom, y.geom) then x.geom
							  ELSE ST_Multi(ST_Intersection(x.geom, y.geom)) END AS geom
					FROM soil.mupolygon AS x
				   INNER JOIN (my_table) AS y ON ST_Intersects(x.geom, y.geom)
				   ) AS z
			) AS a
	  LEFT JOIN soil.coecoclass_mapunit_ranked b ON a.mukey::text = b.mukey::text
	 ORDER BY a.areasymbol, a.musym, b.ecorank) AS q
 GROUP BY ecoclassid_std