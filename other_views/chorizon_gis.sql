-- CREATES a more friendly chorizon table better suited for Related Data navigation in ArcGIS
CREATE VIEW soil.chorizon_gis AS
WITH chtexturegrp_ct AS (
SELECT *
FROM crosstab(
$$
	WITH rv AS (
	SELECT texture, stratextsflag, rvindicator, texdesc, chkey, chtgkey,
			row_number() over(PARTITION BY chkey ORDER BY rvindicator desc, chtgkey) rn
		FROM soil.chtexturegrp 
	)
	SELECT chkey, rn, texdesc FROM rv WHERE rn <= 3 ORDER BY 1,2
$$
)
AS final_result(chkey varchar(30), texdesc1 TEXT, texdesc2 TEXT, texdesc3 TEXT)

), chstructgrp_ct AS (
SELECT *
FROM crosstab(
$$
	WITH rv AS (
	SELECT structgrpname, rvindicator, chkey, chstructgrpkey,
			row_number() over(PARTITION BY chkey ORDER BY rvindicator desc, chstructgrpkey) rn
		FROM soil.chstructgrp
	)
	SELECT chkey, rn, structgrpname FROM rv WHERE rn <= 3 ORDER BY 1,2
$$
)
AS final_result(chkey varchar(30), structgrpname1 varchar(254), structgrpname2 varchar(254), structgrpname3 varchar(254))

), chorizon_concise AS (
SELECT chkey, hzname, hzdept_r, hzdepb_r, fraggt10_r, frag3to10_r, sandtotal_r, silttotal_r, claytotal_r, 
       om_r, ksat_r, awc_r, caco3_r, gypsum_r, sar_r, ec_r, cec7_r, ph1to1h2o_r, cokey
  FROM soil.chorizon
)

SELECT a.cokey, d.compnamelong, a.chkey, a.hzname, a.hzdept_r, a.hzdepb_r, b.texdesc1, b.texdesc2, b.texdesc3,
       c.structgrpname1, c.structgrpname2, c.structgrpname3,
       a.fraggt10_r, a.frag3to10_r, a.sandtotal_r, a.silttotal_r, a.claytotal_r, 
       a.om_r, a.ksat_r, a.awc_r, a.caco3_r, a.gypsum_r, a.sar_r, a.ec_r, a.cec7_r, 
	   a.ph1to1h2o_r 
  FROM chorizon_concise a
  LEFT JOIN chtexturegrp_ct b ON a.chkey = b.chkey
  LEFT JOIN chstructgrp_ct c ON a.chkey = c.chkey
  LEFT JOIN soil.component_mapunit_ranked d ON a.cokey = d.cokey
  ORDER BY a.cokey, a.hzdept_r ASC
