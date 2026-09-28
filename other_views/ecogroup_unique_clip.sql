WITH clip_table AS (
SELECT geom
  FROM some_table
 WHERE some_field = 'something'
 
), clip AS (
SELECT q.areasymbol, q.spatialversion, q.musym, q.mukey,
       CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
            ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
  FROM {schema}.mupolygon AS q
 INNER JOIN clip_table AS r ON ST_Intersects(q.geom, r.geom)

), area_calc AS (
SELECT *, ST_Area(geography(geom))/10000 AS area_ha
  FROM clip

), mu_area_sum AS (
SELECT mukey, sum(area_ha) AS area_ha
    FROM area_calc
GROUP BY mukey

), mu_area_calc AS (
SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype,
       a.ecogrouppct, a.grouprank, (a.ecogrouppct * b.area_ha / 100) AS group_ha
  FROM soil.ecogroup_mapunit_ranked AS a
  INNER JOIN mu_area_sum AS b ON a.mukey = b.mukey
)

SELECT x.ecogroup, COUNT(x.mukey) AS group_n, 
        AVG(CAST(ecogrouppct AS REAL)) AS ecogrouppct_mean, SUM(group_ha) AS group_ha,
        MIN(groupname) AS groupname, MIN(grouptype) AS grouptype
  FROM mu_area_calc AS x
  GROUP BY x.ecogroup
  ORDER BY group_ha DESC;
