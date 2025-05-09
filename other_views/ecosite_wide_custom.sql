/* assumes the following views have been created:
coecoclass_habitat 
component_detail
coecoclass_component_dom */

DROP VIEW IF EXISTS {schema}.ecosite_wide_custom;
CREATE VIEW {schema}.ecosite_wide_custom AS
WITH area AS (
SELECT a.*, b.habitat
  FROM {schema}.coecoclass_unique a 
  LEFT JOIN {schema}.coecoclass_habitat b ON a.ecoclassid_std = b.ecoclassid_std

), dom_comp AS (
SELECT a.ecoclassid_std, a.ecoclassid, a.ecoclassname, a.habitat, 
       a.eco_ha, b.cokey, b.comp_ha, b.comp_ha/a.eco_ha comp_pct
  FROM area a
 LEFT JOIN coecoclass_component_dom b ON a.ecoclassid_std = b.ecoclassid_std

), eco_full AS (
SELECT a.*, b.compname, b.compnamelong, b.compkind,  b.otherph, b.localphase, 
	   b.slope_l, b.slope_r, b.slope_h, b.hydricrating, b.drainagecl,
	   b.elev_l, b.elev_r, b.elev_h, b.aspectrep, b.geomdesc,
	   b.map_l, b.map_r, b.map_h,
	   b.ffd_l, b.ffd_r, b.ffd_h, b.rsprod_l, b.rsprod_r, b.rsprod_h, 
	   b.frostact, b.taxclname, b.taxtempregime, b.taxmoistcl,
	   b.ph1to1h2o_l, b.ph1to1h2o_r, b.ph1to1h2o_h, 
	   b.texdesc, b.texcl, b.pmgroupname, b.comp_habitat, b.comp_prod
  FROM dom_comp a
  LEFT JOIN {schema}.component_detail b ON a.cokey = b.cokey

), eco_translate AS (
SELECT ecoclassid ecosite_id, ecoclassname ecosite_name, 
       round(eco_ha * 2.47105, 0) eco_ac,
	   --substring(ecoclassname from '(\d+)[\-\+]?\d*\s+P\.?Z\.?$') pz_in_l,
	   --substring\(ecoclassname from '\-(\d+)\s+P\.?Z\.?$') pz_in_h,
	   round(cast(map_l AS NUMERIC)/25.4, 1) precip_in_l,
	   round(cast(map_r AS NUMERIC)/25.4, 1) precip_in_r,
	   round(cast(map_h AS NUMERIC)/25.4, 1) precip_in_h,
	   compname, compnamelong,
	   comp_habitat, comp_prod,
	   rsprod_l, rsprod_r, rsprod_h, geomdesc,  
	   round(cast(elev_l as numeric) * 0.0328084, 0) * 100 elev_ft_l, 
	   round(cast(elev_h as numeric) * 0.0328084, 0) * 100 elev_ft_h,
	   CASE WHEN aspectrep BETWEEN 22.6 AND 67.5 THEN 'NE'
	        WHEN aspectrep BETWEEN 67.6 AND 112.5 THEN 'E'
			WHEN aspectrep BETWEEN 112.6 AND 157.5 THEN 'SE'
			WHEN aspectrep BETWEEN 157.6 AND 202.5 THEN 'S'
			WHEN aspectrep BETWEEN 202.6 AND 247.5 THEN 'SW'
			WHEN aspectrep BETWEEN 247.6 AND 292.5 THEN 'W'
			WHEN aspectrep BETWEEN 292.6 AND 337.5 THEN 'NW'
			WHEN aspectrep >= 337.6 OR aspectrep <= 22.5 THEN 'N'
			ELSE NULL END aspect_r,
	   slope_l, slope_h, taxclname, taxtempregime, taxmoistcl, 
	   drainagecl, texcl, pmgroupname, ph1to1h2o_r
  FROM eco_full
)

SELECT * FROM eco_translate ORDER BY eco_ac DESC;
