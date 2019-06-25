SELECT a.areasymbol, a.areaname, b.mukey, b.musym, b.muname, b.mukind, 
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank, d.compkind,
	   d.majcompflag, d.slope_r, d.elev_r, d.aspectrep, d.geomdesc, e.hzname,
	   e.hzdept_r AS hzdept_r_cm, e.hzdepb_r AS hzdepb_r_cm, e.fraggt10_r, e.frag3to10_r,
	   e.sandtotal_r, e.silttotal_r, e.claytotal_r, e.caco3_r, e.gypsum_r, e.ph1to1h2o_r,
	   f.texture_1, f.texture_2, f.texture_3
  FROM legend AS a
  LEFT JOIN mapunit AS b ON a.lkey = b.lkey
  LEFT JOIN component_mapunit_ranked AS c ON b.mukey = c.mukey
  LEFT JOIN component AS d ON c.cokey = d.cokey
  LEFT JOIN chorizon AS e ON d.cokey = e.cokey
  LEFT JOIN chtexturegrp_crosstab AS f ON e.chkey = f.chkey
 ORDER BY a.areasymbol, b.musym, c.comprank, e.hzdept_r