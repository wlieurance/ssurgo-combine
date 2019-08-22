-- clips mupolygon by a table and returns the sum of the ecogroups and ecogroup area in the clipped result.
-- replace 'my_table' with actual table to clip by
SELECT ecogroup, grouptype, max(groupname) AS groupname, sum(ecogroup_ha) AS ecogroup_ha
  FROM (
        SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha, b.ecogroup,
               b.grouptype, b.pub_status, b.ecogrouppct, b.grouprank, b.groupname,
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
	   ) as c
 GROUP BY ecogroup, grouptype, pub_status
 ORDER BY grouptype, ecogroup
