--neads tablefunc extention for crosstab
--CREATE EXTENSION tablefunc;
WITH chtexturegrp_crosstab AS (
SELECT * 
  FROM crosstab (
       'SELECT chkey, row_num, texdesc
          FROM (
               SELECT chkey, rvindicator, texdesc, 
                      Row_Number() Over(PARTITION BY chkey ORDER BY rvindicator DESC, texdesc ASC) AS row_num
                 FROM soil.chtexturegrp) AS a
         ORDER BY 1, 2')
    AS chtexturegrp_crosstab(chkey VARCHAR (30), texture_1 TEXT, texture_2 TEXT, texture_3 TEXT)

), complist AS (
SELECT a.areasymbol, a.areaname, b.mukey, b.musym, 
       NULLIF(regexp_replace(b.musym, '\D','','g'), '')::numeric AS musym_numeric,
       b.muname, b.mukind, 
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank, d.compkind,
       d.majcompflag, d.slope_r, d.elev_r, d.aspectrep, d.geomdesc, e.hzname,
       e.hzdept_r AS hzdept_r_cm, e.hzdepb_r AS hzdepb_r_cm, e.frag3to10_r AS frag3to10_r_in, e.fraggt10_r AS fraggt10_r_in,
       e.sandtotal_r, e.silttotal_r, e.claytotal_r, e.caco3_r, e.gypsum_r, e.ph1to1h2o_r,
       f.texture_1, f.texture_2, f.texture_3
  FROM soil.legend AS a
  LEFT JOIN soil.mapunit AS b ON a.lkey = b.lkey
  LEFT JOIN soil.component_mapunit_ranked AS c ON b.mukey = c.mukey
  LEFT JOIN soil.component AS d ON c.cokey = d.cokey
  LEFT JOIN soil.chorizon AS e ON d.cokey = e.cokey
  LEFT JOIN chtexturegrp_crosstab AS f ON e.chkey = f.chkey

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
 --WHERE a.majcompflag LIKE 'No%'
 ORDER BY a.areasymbol, a.musym_numeric, a.musym, a.comprank, a.hzdept_r_cm