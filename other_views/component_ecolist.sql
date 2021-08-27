WITH complist AS (
SELECT a.areasymbol, a.areaname, b.mukey, b.musym, 
       NULLIF(regexp_replace(b.musym, '\D','','g'), '')::numeric AS musym_numeric,
       b.muname, b.mukind, 
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank, d.compkind,
       d.majcompflag,  d.slope_r, d.elev_r, d.aspectrep, d.geomdesc,
       e.ecoclasstypename, e.ecoclassref, e.ecoclassid, e.ecoclassid_std, 
       e.ecoclassname, e.ecoclassname_std, f.ecogroup
  FROM soil.legend AS a
  LEFT JOIN soil.mapunit AS b ON a.lkey = b.lkey
  LEFT JOIN soil.component_mapunit_ranked AS c ON b.mukey = c.mukey
  LEFT JOIN soil.component AS d ON c.cokey = d.cokey
  LEFT JOIN soil.coecoclass_codominant AS e ON c.cokey = e.cokey
  LEFT JOIN soil.ecogroup AS f ON e.ecoclassid_std = f.ecoid
    
), clip AS (
SELECT q.areasymbol, q.spatialver, q.musym, q.mukey,
       CASE WHEN ST_CoveredBy(q.geom, r.geom) THEN q.geom
            ELSE ST_Multi(ST_Intersection(q.geom, r.geom)) END AS geom
  FROM soil.mupolygon AS q
 INNER JOIN (SELECT * FROM my_table WHERE condition = '') AS r ON ST_Intersects(q.geom, r.geom)
)

SELECT a.* 
  FROM complist AS a
 INNER JOIN clip AS b ON a.mukey = b.mukey
 ORDER BY a.areasymbol, a.musym_numeric, a.musym, a.comprank