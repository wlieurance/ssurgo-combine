-- returns a list of map units, their associated ecogroups and the theoretical area of each ecogroup/ecosite in that area in hectares.
-- replace 'my_table' with actual table to clip by
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha, b.ecogroup,
       b.grouptype, b.pub_status, b.ecogrouppct, b.grouprank,
       a.area_ha * b.ecogrouppct::double precision / 100::double precision AS ecogroup_ha
  FROM (
	   SELECT z.areasymbol, z.spatialver, z.musym, z.mukey,
	           ST_Area(Geography(z.geom))/10000 AS area_ha, z.geom
	     FROM (
			  SELECT x.areasymbol, x.spatialver, x.musym, x.mukey,
					 CASE WHEN ST_CoveredBy(x.geom, y.geom) then x.geom
						  ELSE ST_Multi(ST_Intersection(x.geom, y.geom)) END AS geom
				FROM soil.mupolygon AS x
			   INNER JOIN my_table AS y ON ST_Intersects(x.geom, y.geom)
			   ) AS z
	    ) AS a
  LEFT JOIN soil.ecogroup_mapunit_ranked b ON a.mukey::text = b.mukey::text
 ORDER BY a.areasymbol, a.musym, b.grouprank;