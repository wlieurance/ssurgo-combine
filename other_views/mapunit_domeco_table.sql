SELECT row_number() over(ORDER BY a.lkey, b.mukey, c.ecorank) AS fid,
SELECT a.areasymbol, a.areaname, b.musym, b.muname, b.mukind, b.muacres, b.mukey, 
       c.ecoclassid, c.ecoclassid_std, c.ecoclassname, c.ecoclassname_std, c.ecotype, c.ecoclasspct, c.ecorank
  FROM legend AS a
  LEFT JOIN mapunit AS b on a.lkey = b.lkey
  LEFT JOIN coecoclass_mapunit_ranked AS c ON b.mukey = c.mukey
  