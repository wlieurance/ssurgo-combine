SELECT row_number() over(ORDER BY a.lkey, b.mukey, d.comprank) AS fid,
       a.areasymbol, a.areaname, b.musym, b.muname, b.mukind, b.muacres, b.mukey, 
       c.comppct_l, c.comppct_r, c.comppct_h, c.compname, c.compkind, c.majcompflag, c.otherph, c.localphase, 
       c.slope_l, c.slope_r, c.slope_h, c.runoff, c.tfact, c.wei, c.weg, c.hydricrating, c.drainagecl, c.elev_l, 
       c.elev_r, c.elev_h, c.aspectrep, c.geomdesc, c.albedodry_r, c.airtempa_r, c.map_r, c.ffd_r, c.nirrcapcl, c.nirrcapscl, 
       c.nirrcapunit, c.irrcapcl, c.irrcapscl, c.irrcapunit, c.rsprod_l, c.rsprod_r, c.rsprod_h, c.frostact, 
       c.hydgrp, c.corcon, c.corsteel, c.taxclname, c.taxorder, c.taxsuborder, c.taxgrtgroup, c.taxsubgrp, c.taxpartsize, 
       c.taxpartsizemod, c.taxceactcl, c.taxreaction, c.taxtempcl, c.taxmoistscl, c.taxtempregime, c.soiltaxedition, c.cokey,
       d.compnamelong, d.comprank, e.ecoclasstypename, e.ecoclassref, e.ecoclassid, e.ecoclassname, e.ecoclassid_std, e.ecoclassname_std,
       e.coecoclasskey
  FROM legend AS a
  LEFT JOIN mapunit AS b on a.lkey = b.lkey
  LEFT JOIN component AS c ON b.mukey = c.mukey
  LEFT JOIN component_mapunit_ranked AS d ON c.cokey = d.cokey
  LEFT JOIN coecoclass_codominant AS e ON c.cokey = e.cokey