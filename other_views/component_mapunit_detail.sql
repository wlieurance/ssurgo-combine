CREATE VIEW soil.component_mapunit_detail AS
SELECT a.areasymbol, a.areaname, b.musym, b.muname, b.mukind, b.muacres, b.mukey, 
       c.cokey, c.compname, c.compnamelong, c.comppct_r, c.comprank,
       e.ecoclassid, e.ecoclassid_std, e.ecoclassname, f.habitat, f.prod,
       d.compkind, d.majcompflag, d.otherph, d.localphase, d.slope_l, d.slope_r, d.slope_h, d.slopelenusle_l, d.slopelenusle_r, d.slopelenusle_h, 
       d.runoff, d.tfact, d.wei, d.weg, d.erocl, d.earthcovkind1, d.earthcovkind2, d.hydricon, d.hydricrating, d.drainagecl, d.elev_l, d.elev_r, d.elev_h, 
       d.aspectccwise, d.aspectrep, d.aspectcwise, d.geomdesc, d.albedodry_l, d.albedodry_r, d.albedodry_h, d.airtempa_l, d.airtempa_r, d.airtempa_h, 
       d.map_l, d.map_r, d.map_h, d.reannualprecip_l, d.reannualprecip_r, d.reannualprecip_h, d.ffd_l, d.ffd_r, d.ffd_h, d.nirrcapcl, d.nirrcapscl, 
       d.nirrcapunit, d.irrcapcl, d.irrcapscl, d.irrcapunit, d.cropprodindex, d.constreeshrubgrp, d.wndbrksuitgrp, d.rsprod_l, d.rsprod_r, d.rsprod_h, 
       d.foragesuitgrpid, d.wlgrain, d.wlgrass, d.wlherbaceous, d.wlshrub, d.wlconiferous, d.wlhardwood, d.wlwetplant, d.wlshallowwat, d.wlrangeland, d.wlopenland, 
       d.wlwoodland, d.wlwetland, d.soilslippot, d.frostact, d.initsub_l, d.initsub_r, d.initsub_h, d.totalsub_l, d.totalsub_r, d.totalsub_h, d.hydgrp, d.corcon, 
       d.corsteel, d.taxclname, d.taxorder, d.taxsuborder, d.taxgrtgroup, d.taxsubgrp, d.taxpartsize, d.taxpartsizemod, d.taxceactcl, d.taxreaction, d.taxtempcl, 
       d.taxmoistscl, d.taxtempregime, d.soiltaxedition, d.castorieindex, d.flecolcomnum, d.flhe, d.flphe, d.flsoilleachpot, d.flsoirunoffpot, d.fltemik2use, 
       d.fltriumph2use, d.indraingrp, d.innitrateleachi, d.misoimgmtgrp, d.vasoimgtgrp
  FROM soil.legend AS a
  LEFT JOIN soil.mapunit AS b on a.lkey = b.lkey 
  LEFT JOIN soil.component_mapunit_ranked c ON b.mukey = c.mukey
  LEFT JOIN soil.component d ON c.cokey = d.cokey
  LEFT JOIN soil.coecoclass_codominant e ON c.cokey = e.cokey
  LEFT JOIN soil.component_habitat f ON c.cokey = f.cokey;