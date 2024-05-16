CREATE VIEW soil.component_detail AS
WITH texture AS (
SELECT a.cokey, a.chkey, a.ph1to1h2o_l, a.ph1to1h2o_r, a.ph1to1h2o_h, b.texdesc, b.texcl
  FROM soil.chorizon_surface_noom a
 INNER JOIN soil.texture_first b ON a.chkey = b.chkey
)

SELECT a.*,
	    CONCAT(a.compname,
	    CASE WHEN a.localphase IS NULL THEN '' 
		     ELSE ' ' || a.localphase END,
	    CASE WHEN b.texdesc IS NULL THEN '' 
		     ELSE ' ' || LOWER(b.texdesc) END,
	    CASE WHEN a.slope_l IS NULL THEN '' 
		     ELSE CONCAT(', ',
					     CAST(a.slope_l AS INTEGER),
					     ' to ',
					     CAST(a.slope_h AS INTEGER),
					     '% slope') 
					     END) compnamelong,
	   b.ph1to1h2o_l, b.ph1to1h2o_r, b.ph1to1h2o_h,
	   b.texdesc, b.texcl, c.pmgroupname,
	   d.ecoclassid, d.ecoclassid_std, d.ecoclassname, 
	   e.habitat comp_habitat, e.prod comp_prod
  FROM soil.component a
  LEFT JOIN texture b ON a.cokey = b.cokey
  LEFT JOIN soil.copmgrp_first c ON a.cokey = c.cokey
  LEFT JOIN soil.coecoclass_codominant d ON a.cokey = d.cokey
  LEFT JOIN soil.component_habitat e ON a.cokey = e.cokey;
  
	
