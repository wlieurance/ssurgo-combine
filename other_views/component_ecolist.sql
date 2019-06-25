SELECT a.areasymbol, a.areaname, b.mukey, b.musym, b.muname, b.mukind, 
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank, d.compkind,
	   d.majcompflag, e.ecoclasstypename, e.ecoclassref, e.ecoclassid, e.ecoclassid_std, 
	   e.ecoclassname, e.ecoclassname_std, f.ecogroup, f.group_type, f.modal
  FROM legend AS a
  LEFT JOIN mapunit AS b ON a.lkey = b.lkey
  LEFT JOIN component_mapunit_ranked AS c ON b.mukey = c.mukey
  LEFT JOIN component AS d ON c.cokey = d.cokey
  LEFT JOIN coecoclass_codominant AS e ON c.cokey = e.cokey
  LEFT JOIN ecogroup AS f ON e.ecoclassid_std = f.ecoid
 ORDER BY a.areasymbol, b.musym, c.comprank