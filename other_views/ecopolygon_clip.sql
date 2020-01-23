-- returns the dominant ecosite polygons with correct percentages from the area
-- replace 'my_table' with actual table to clip by
SELECT ecoclassid_std, ecoclassname, ecotype, area_ha, ecoarea_ha/area_ha AS ecopct, ST_ForceRHR(ST_MakeValid(geom)) AS geom
  FROM (
       SELECT ecoclassid_std, ecoclassname, ecotype, ecoarea_ha, ST_Area(geography(geom))/10000 AS area_ha,
              ST_Multi(geom) AS geom
         FROM (
              SELECT ST_Union(geom) AS geom, ecoclassid_std, min(ecoclassname) AS ecoclassname, min(ecotype) AS ecotype, sum(ecoarea_ha) AS ecoarea_ha
                FROM (
                     SELECT COALESCE(b.ecoclassid_std, 'NA') AS ecoclassid_std, b.ecoclassname, b.ecotype, 
                            a.area_ha*(CAST(b.ecoclasspct AS NUMERIC)/100) AS ecoarea_ha, a.geom
                       FROM (
                            SELECT s.areasymbol, s.spatialver, s.musym, s.mukey, s.geom,
                                   ST_Area(geography(s.geom))/10000 AS area_ha
                              FROM (
                                   SELECT q.areasymbol, q.spatialver, q.musym, q.mukey,
                                          CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
                                          ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
                                     FROM soil.mupolygon AS q
                                    INNER JOIN (my_table) AS r ON ST_Intersects(q.geom, r.geom)
                                    ) AS s
                            ) AS a
                       LEFT JOIN soil.coecoclass_mudominant AS b ON a.mukey = b.mukey)
                     AS x
               GROUP BY ecoclassid_std) AS y)
       AS z;