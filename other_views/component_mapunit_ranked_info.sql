SELECT a.mukey, a.cokey, a.compname, a.compnamelong, a.comppct_r, a.comprank,
       b.ecoclassid, b.ecoclassid_std, b.ecoclassname, c.rangeprod, c.forestunprod,
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
  FROM soil.component_mapunit_ranked a
  LEFT JOIN soil.coecoclass_codominant b ON a.cokey = b.cokey
  LEFT JOIN soil.coeplants_delimited c ON a.cokey = c.cokey
  LEFT JOIN soil.component d ON a.cokey = d.cokey