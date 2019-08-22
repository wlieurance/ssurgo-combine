SELECT a.*, b.muname, b.mukind, b.muacres, c.ecoclassid, c.ecoclassid_std AS ecoidstd, c.ecoclassname AS ecoclassnm, c.ecoclassname_std AS econamestd, c.ecotype, c.ecoclasspct AS ecopct, 
            d.compname, d.compnamelong AS compnmlong, d.comppct_r, d.cokey, e.compkind, e.otherph, e.localphase, e.slope_l, e.slope_r, e.slope_h,
            e.runoff, e.drainagecl, e.elev_r, e.aspectrep, e.geomdesc, e.taxclname
  FROM mupolygon AS a
  LEFT JOIN mapunit AS b ON a .mukey = b.mukey
  LEFT JOIN coecoclass_mudominant AS c ON a.mukey = c.mukey
  LEFT JOIN component_dominant AS d ON a.mukey = d.mukey
  LEFT JOIN component AS e ON d.cokey = e.cokey