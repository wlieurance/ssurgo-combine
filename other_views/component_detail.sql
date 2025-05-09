-- SQLite
-- DROP TABLE IF EXISTS {schema}.component_detail;
-- CREATE TABLE {schema}.component_detail AS

DROP VIEW IF EXISTS {schema}.component_detail;
CREATE VIEW {schema}.component_detail AS
WITH texture AS (
SELECT a.cokey, a.chkey, a.ph1to1h2o_l, a.ph1to1h2o_r, a.ph1to1h2o_h, b.texdesc, b.texcl
  FROM {schema}.chorizon_surface_noom a
 INNER JOIN {schema}.texture_first b ON a.chkey = b.chkey
)

SELECT a.comppct_l, a.comppct_r, a.comppct_h, a.compname, a.compkind, a.majcompflag,
       a.otherph, a.localphase, a.slope_l, a.slope_r, a.slope_h, a.slopelenusle_l,
       a.slopelenusle_r, a.slopelenusle_h, a.runoff, a.tfact, a.wei, a.weg, a.erocl,
       a.earthcovkind1, a.earthcovkind2, a.hydricon, a.hydricrating, a.drainagecl,
       a.elev_l, a.elev_r, a.elev_h, a.aspectccwise, a.aspectrep, a.aspectcwise,
       a.geomdesc, a.albedodry_l, a.albedodry_r, a.albedodry_h, a.airtempa_l,
       a.airtempa_r, a.airtempa_h, a.map_l, a.map_r, a.map_h, a.reannualprecip_l,
       a.reannualprecip_r, a.reannualprecip_h, a.ffd_l, a.ffd_r, a.ffd_h, a.nirrcapcl,
       a.nirrcapscl, a.nirrcapunit, a.irrcapcl, a.irrcapscl, a.irrcapunit, a.cropprodindex,
       a.constreeshrubgrp, a.wndbrksuitgrp, a.rsprod_l, a.rsprod_r, a.rsprod_h,
       a.foragesuitgrpid, a.wlgrain, a.wlgrass, a.wlherbaceous, a.wlshrub, a.wlconiferous,
       a.wlhardwood, a.wlwetplant, a.wlshallowwat, a.wlrangeland, a.wlopenland, a.wlwoodland,
       a.wlwetland, a.soilslippot, a.frostact, a.initsub_l, a.initsub_r, a.initsub_h,
       a.totalsub_l, a.totalsub_r, a.totalsub_h, a.hydgrp, a.corcon, a.corsteel,
       a.taxclname, a.taxorder, a.taxsuborder, a.taxgrtgroup, a.taxsubgrp, a.taxpartsize,
       a.taxpartsizemod, a.taxceactcl, a.taxreaction, a.taxtempcl, a.taxmoistscl, f.taxmoistcl,
       a.taxtempregime, a.soiltaxedition, a.castorieindex, a.flecolcomnum, a.flhe,
       a.flphe, a.flsoilleachpot, a.flsoirunoffpot, a.fltemik2use, a.fltriumph2use,
       a.indraingrp, a.innitrateleachi, a.misoimgmtgrp, a.vasoimgtgrp,
       concat(
	     concat_ws(', ',
		   concat_ws(' ', a.compname, a.localphase, LOWER(b.texdesc)),
		   concat_ws(' to ', CAST(a.slope_l AS INTEGER), CAST(a.slope_h AS INTEGER))
		 ), CASE WHEN a.slope_l IS NULL AND a.slope_h IS NULL THEN NULL ELSE '% slope' END
	   ) AS compnamelong,
	   b.ph1to1h2o_l, b.ph1to1h2o_r, b.ph1to1h2o_h,
	   b.texdesc, b.texcl, c.pmgroupname,
	   d.ecoclassid, d.ecoclassid_std, d.ecoclassname, 
	   e.habitat comp_habitat, e.prod comp_prod, a.mukey, a.cokey
  FROM {schema}.component a
  LEFT JOIN texture b ON a.cokey = b.cokey
  LEFT JOIN {schema}.copmgrp_first c ON a.cokey = c.cokey
  LEFT JOIN {schema}.coecoclass_codominant d ON a.cokey = d.cokey
  LEFT JOIN {schema}.component_habitat e ON a.cokey = e.cokey
  LEFT JOIN {schema}.cotaxmoistcl_first f ON a.cokey = f.cokey;
	
