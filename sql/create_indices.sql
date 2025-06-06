-- Create Foreign Key column indices to speed up queries and cascade deletes.  
CREATE INDEX IF NOT EXISTS featdesc_areasymbol_idx ON {schema}.featdesc (areasymbol);
CREATE INDEX IF NOT EXISTS featline_areasymbol_idx ON {schema}.featline (areasymbol);
CREATE INDEX IF NOT EXISTS featpoint_areasymbol_idx ON {schema}.featpoint (areasymbol);
CREATE INDEX IF NOT EXISTS legend_areasymbol_idx ON {schema}.legend (areasymbol);
CREATE INDEX IF NOT EXISTS mdstatdomdet_domainname_idx ON {schema}.mdstatdomdet (domainname);
CREATE INDEX IF NOT EXISTS mdstatidxmas_tabphyname_idx ON {schema}.mdstatidxmas (tabphyname);
CREATE INDEX IF NOT EXISTS mdstatrshipmas_ltabphyname_idx ON {schema}.mdstatrshipmas (ltabphyname);
CREATE INDEX IF NOT EXISTS mdstatrshipmas_rtabphyname_idx ON {schema}.mdstatrshipmas (rtabphyname);
CREATE INDEX IF NOT EXISTS mdstattabcols_tabphyname_idx ON {schema}.mdstattabcols (tabphyname);
CREATE INDEX IF NOT EXISTS mdstattabcols_domainname_idx ON {schema}.mdstattabcols (domainname);
CREATE INDEX IF NOT EXISTS sdvfolderattribute_folderkey_idx ON {schema}.sdvfolderattribute (folderkey);
CREATE INDEX IF NOT EXISTS sdvfolderattribute_attributekey_idx ON {schema}.sdvfolderattribute (attributekey);
CREATE INDEX IF NOT EXISTS sapolygon_areasymbol_idx ON {schema}.sapolygon (areasymbol); --not fkey
CREATE INDEX IF NOT EXISTS distlegendmd_lkey_idx ON {schema}.distlegendmd (lkey);
CREATE INDEX IF NOT EXISTS distinterpmd_distmdkey_idx ON {schema}.distinterpmd (distmdkey);
CREATE INDEX IF NOT EXISTS laoverlap_lkey_idx ON {schema}.laoverlap (lkey);
CREATE INDEX IF NOT EXISTS legendtext_lkey_idx ON {schema}.legendtext (lkey);
CREATE INDEX IF NOT EXISTS mapunit_lkey_idx ON {schema}.mapunit (lkey);
CREATE INDEX IF NOT EXISTS mdstatidxdet_fkey_idx ON {schema}.mdstatidxdet (tabphyname, idxphyname);
CREATE INDEX IF NOT EXISTS mdstatrshipdet_fkey_idx ON {schema}.mdstatrshipdet (ltabphyname, rtabphyname, relationshipname);
CREATE INDEX IF NOT EXISTS sainterp_sacatalogkey_idx ON {schema}.sainterp (sacatalogkey);
CREATE INDEX IF NOT EXISTS component_mukey_idx ON {schema}.component (mukey);
CREATE INDEX IF NOT EXISTS muaoverlap_mukey_idx ON {schema}.muaoverlap (mukey);
CREATE INDEX IF NOT EXISTS mucropyld_mukey_idx ON {schema}.mucropyld (mukey);
CREATE INDEX IF NOT EXISTS muline_areasymbol_idx ON {schema}.muline (areasymbol); --not fkey
CREATE INDEX IF NOT EXISTS mupoint_areasymbol_idx ON {schema}.mupoint (areasymbol);  --not fkey
CREATE INDEX IF NOT EXISTS mupolygon_areasymbol_idx ON {schema}.mupolygon (areasymbol); --not fkey
CREATE INDEX IF NOT EXISTS mutext_mukey_idx ON {schema}.mutext (mukey);
CREATE INDEX IF NOT EXISTS chorizon_cokey_idx ON {schema}.chorizon (cokey);
CREATE INDEX IF NOT EXISTS cocanopycover_cokey_idx ON {schema}.cocanopycover (cokey);
CREATE INDEX IF NOT EXISTS cocropyld_cokey_idx ON {schema}.cocropyld (cokey);
CREATE INDEX IF NOT EXISTS codiagfeatures_cokey_idx ON {schema}.codiagfeatures (cokey);
CREATE INDEX IF NOT EXISTS coecoclass_cokey_idx ON {schema}.coecoclass (cokey);
CREATE INDEX IF NOT EXISTS coeplants_cokey_idx ON {schema}.coeplants (cokey);
CREATE INDEX IF NOT EXISTS coerosionacc_cokey_idx ON {schema}.coerosionacc (cokey);
CREATE INDEX IF NOT EXISTS coforprod_cokey_idx ON {schema}.coforprod (cokey);
CREATE INDEX IF NOT EXISTS cogeomordesc_cokey_idx ON {schema}.cogeomordesc (cokey);
CREATE INDEX IF NOT EXISTS cohydriccriteria_cokey_idx ON {schema}.cohydriccriteria (cokey);
CREATE INDEX IF NOT EXISTS cointerp_cokey_idx ON {schema}.cointerp (cokey);
CREATE INDEX IF NOT EXISTS comonth_cokey_idx ON {schema}.comonth (cokey);
CREATE INDEX IF NOT EXISTS copmgrp_cokey_idx ON {schema}.copmgrp (cokey);
CREATE INDEX IF NOT EXISTS copwindbreak_cokey_idx ON {schema}.copwindbreak (cokey);
CREATE INDEX IF NOT EXISTS corestrictions_cokey_idx ON {schema}.corestrictions (cokey);
CREATE INDEX IF NOT EXISTS cosurffrags_cokey_idx ON {schema}.cosurffrags (cokey);
CREATE INDEX IF NOT EXISTS cotaxfmmin_cokey_idx ON {schema}.cotaxfmmin (cokey);
CREATE INDEX IF NOT EXISTS cotaxmoistcl_cokey_idx ON {schema}.cotaxmoistcl (cokey);
CREATE INDEX IF NOT EXISTS cotext_cokey_idx ON {schema}.cotext (cokey);
CREATE INDEX IF NOT EXISTS cotreestomng_cokey_idx ON {schema}.cotreestomng (cokey);
CREATE INDEX IF NOT EXISTS cotxfmother_cokey_idx ON {schema}.cotxfmother (cokey);
CREATE INDEX IF NOT EXISTS chaashto_chkey_idx ON {schema}.chaashto (chkey);
CREATE INDEX IF NOT EXISTS chconsistence_chkey_idx ON {schema}.chconsistence (chkey);
CREATE INDEX IF NOT EXISTS chdesgnsuffix_chkey_idx ON {schema}.chdesgnsuffix (chkey);
CREATE INDEX IF NOT EXISTS chfrags_chkey_idx ON {schema}.chfrags (chkey);
CREATE INDEX IF NOT EXISTS chpores_chkey_idx ON {schema}.chpores (chkey);
CREATE INDEX IF NOT EXISTS chstructgrp_chkey_idx ON {schema}.chstructgrp (chkey);
CREATE INDEX IF NOT EXISTS chtext_chkey_idx ON {schema}.chtext (chkey);
CREATE INDEX IF NOT EXISTS chtexturegrp_chkey_idx ON {schema}.chtexturegrp (chkey);
CREATE INDEX IF NOT EXISTS chunified_chkey_idx ON {schema}.chunified (chkey);
CREATE INDEX IF NOT EXISTS coforprodo_cofprodkey_idx ON {schema}.coforprodo (cofprodkey);
CREATE INDEX IF NOT EXISTS copm_copmgrpkey_idx ON {schema}.copm (copmgrpkey);
CREATE INDEX IF NOT EXISTS cosoilmoist_comonthkey_idx ON {schema}.cosoilmoist (comonthkey);
CREATE INDEX IF NOT EXISTS cosoiltemp_comonthkey_idx ON {schema}.cosoiltemp (comonthkey);
CREATE INDEX IF NOT EXISTS cosurfmorphgc_cogeomdkey_idx ON {schema}.cosurfmorphgc (cogeomdkey);
CREATE INDEX IF NOT EXISTS cosurfmorphhpp_cogeomdkey_idx ON {schema}.cosurfmorphhpp (cogeomdkey);
CREATE INDEX IF NOT EXISTS cosurfmorphmr_cogeomdkey_idx ON {schema}.cosurfmorphmr (cogeomdkey);
CREATE INDEX IF NOT EXISTS cosurfmorphss_cogeomdkey_idx ON {schema}.cosurfmorphss (cogeomdkey);
CREATE INDEX IF NOT EXISTS chstruct_chstructgrpkey_idx ON {schema}.chstruct (chstructgrpkey);
CREATE INDEX IF NOT EXISTS chtexture_chtgkey_idx ON {schema}.chtexture (chtgkey);
CREATE INDEX IF NOT EXISTS chtexturemod_chtkey_idx ON {schema}.chtexturemod (chtkey);

-- Spatial indices
CREATE INDEX IF NOT EXISTS mupolygon_gix ON {schema}.mupolygon USING GIST (geom);
CREATE INDEX IF NOT EXISTS muline_gix ON {schema}.muline USING GIST (geom);
CREATE INDEX IF NOT EXISTS mupoint_gix ON {schema}.mupoint USING GIST (geom);
CREATE INDEX IF NOT EXISTS sapolygon_gix ON {schema}.sapolygon USING GIST (geom);
CREATE INDEX IF NOT EXISTS featline_gix ON {schema}.featline USING GIST (geom);
CREATE INDEX IF NOT EXISTS featpoint_gix ON {schema}.featpoint USING GIST (geom);
