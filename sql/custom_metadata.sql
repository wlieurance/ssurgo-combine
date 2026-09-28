/*
Custom documentation for SSURGO-derived relations.

This file intentionally documents custom relations and fields created by the
SQL in this directory. Pass-through SSURGO fields retain their definitions from
the SSURGO metadata tables and are not repeated here.

Replace {schema} through the same templating mechanism used by the creation SQL.
*/

-- User-maintained ecological grouping tables

COMMENT ON TABLE {schema}.ecogroup_meta IS
'Describes the characteristics of each ecological grouping.';

COMMENT ON COLUMN {schema}.ecogroup_meta.ecogroup IS
'Unique alphanumeric identifier for the ecogroup.';
COMMENT ON COLUMN {schema}.ecogroup_meta.groupname IS
'Descriptive name, if any, assigned to the ecogroup.';
COMMENT ON COLUMN {schema}.ecogroup_meta.veg_type IS
'Vegetation type in TREE1-TREE2/SHRUB1-SHRUB2/HERB1-HERB2 format, ordered by production dominance; shrub includes shrubs and subshrubs, HERB denotes perennial herbaceous plants, and the value is generally derived from the group''s modal ecological site description.';
COMMENT ON COLUMN {schema}.ecogroup_meta.grouptype IS
'Type of ecological grouping represented by the record.';
COMMENT ON COLUMN {schema}.ecogroup_meta.modal_site IS
'NRCS Ecological Classification ID of the site having the greatest area in the group, being most representative of its response to disturbance, or both.';
COMMENT ON COLUMN {schema}.ecogroup_meta.published IS
'Indicates whether the ecological-site grouping and, optionally, its State-and-Transition Model have been published and distributed.';
COMMENT ON COLUMN {schema}.ecogroup_meta.modeled IS
'Indicates whether the group has been modeled with a State-and-Transition Model.';
COMMENT ON COLUMN {schema}.ecogroup_meta.model_status IS
'Publication status of the State-and-Transition Model associated with the grouping.';

COMMENT ON TABLE {schema}.ecogroup IS
'Defines which ecological sites belong to which ecogroups.';

COMMENT ON COLUMN {schema}.ecogroup.ecoid IS
'NRCS Ecological Classification ID of the ecological site.';
COMMENT ON COLUMN {schema}.ecogroup.ecogroup IS
'Ecogroup identifier associated with the ecological site.';

-- Ecological grouping views

COMMENT ON VIEW {schema}.ecogroup_detail IS
'Lists ecological sites assigned to ecological groups, joins the descriptive group metadata, and identifies each group''s modal ecological site.';

COMMENT ON COLUMN {schema}.ecogroup_detail.modal IS
'Equals 1 when the ecological site is the modal site for its ecological group, 0 when it is a nonmodal member, and NULL when no matching group metadata exists.';

COMMENT ON VIEW {schema}.ecogroup_mapunit_ranked IS
'Aggregates ecological-site component percentages into ecological groups for each map unit and ranks the resulting groups from greatest to least representative percentage.';

COMMENT ON COLUMN {schema}.ecogroup_mapunit_ranked.ecogroup IS
'Ecological-group identifier; when an ecological site has no group assignment, its standardized ecological-site identifier is used.';
COMMENT ON COLUMN {schema}.ecogroup_mapunit_ranked.groupname IS
'Ecological-group name; for an ungrouped ecological site, a normalized ecological-site name is used.';
COMMENT ON COLUMN {schema}.ecogroup_mapunit_ranked.grouptype IS
'Classification of the result as a defined ecological group or an ungrouped ecological site type.';
COMMENT ON COLUMN {schema}.ecogroup_mapunit_ranked.ecogrouppct IS
'Sum of representative map-unit component percentages assigned to the ecological group or ungrouped ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_mapunit_ranked.grouprank IS
'Rank of the ecological group within the map unit, ordered by descending representative percentage with deterministic type and identifier tie breakers.';

COMMENT ON VIEW {schema}.ecogroup_mudominant IS
'Returns the highest-ranked ecological group or ecological site for each map unit.';

COMMENT ON VIEW {schema}.ecogroup_unique IS
'Summarizes each ecological group or ungrouped ecological site across map units, including occurrence count, mean representative percentage, and estimated mapped hectares.';

COMMENT ON COLUMN {schema}.ecogroup_unique.group_n IS
'Number of map units in which the ecological group or ungrouped ecological site occurs.';
COMMENT ON COLUMN {schema}.ecogroup_unique.ecogrouppct_mean IS
'Mean representative percentage of the ecological group across map units in which it occurs.';
COMMENT ON COLUMN {schema}.ecogroup_unique.group_ha IS
'Estimated hectares assigned to the ecological group, calculated from map-unit polygon area and representative component percentage.';

COMMENT ON VIEW {schema}.ecogroup_wide IS
'Pivots the six highest-ranked ecological groups or ecological sites for each map unit into one wide record suitable for display or joining by map-unit key.';

COMMENT ON COLUMN {schema}.ecogroup_wide.ecogroup_1 IS 'Identifier of the first-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.groupname_1 IS 'Name of the first-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogrouppct_1 IS 'Representative percentage of the first-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogroup_2 IS 'Identifier of the second-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.groupname_2 IS 'Name of the second-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogrouppct_2 IS 'Representative percentage of the second-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogroup_3 IS 'Identifier of the third-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.groupname_3 IS 'Name of the third-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogrouppct_3 IS 'Representative percentage of the third-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogroup_4 IS 'Identifier of the fourth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.groupname_4 IS 'Name of the fourth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogrouppct_4 IS 'Representative percentage of the fourth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogroup_5 IS 'Identifier of the fifth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.groupname_5 IS 'Name of the fifth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogrouppct_5 IS 'Representative percentage of the fifth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogroup_6 IS 'Identifier of the sixth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.groupname_6 IS 'Name of the sixth-ranked ecological group or ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_wide.ecogrouppct_6 IS 'Representative percentage of the sixth-ranked ecological group or ecological site.';

COMMENT ON VIEW {schema}.ecogroup_area IS
'Combines map-unit polygons with ranked ecological groups and estimates the hectares represented by each ecological group within each polygon.';

COMMENT ON COLUMN {schema}.ecogroup_area.ecogroup_ha IS
'Estimated hectares of the polygon represented by the ecological group, calculated from polygon area and representative group percentage.';

COMMENT ON VIEW {schema}.ecogroup_plantprod IS
'Estimates component-area-weighted plant production percentages by ecological group or ungrouped ecological site, plant, and production category.';

COMMENT ON COLUMN {schema}.ecogroup_plantprod.ecogroup IS
'Ecological-group identifier; when an ecological site has no group assignment, its standardized ecological-site identifier is used.';
COMMENT ON COLUMN {schema}.ecogroup_plantprod.grouptype IS
'Indicates whether the summarized identifier represents an ecological group or an ungrouped ecological site.';
COMMENT ON COLUMN {schema}.ecogroup_plantprod.prodtype IS
'Plant-production category used in the estimate: range, forest understory, or forest overstory.';
COMMENT ON COLUMN {schema}.ecogroup_plantprod.prod IS
'Component-area-weighted mean percentage attributed to the plant within the ecological group or ecological site and production category.';

-- Materialized spatial query results

COMMENT ON TABLE {schema}.ecogrouppolygon IS
'Represents the dominant ecogroup, or the dominant ecological site where no ecogroup is assigned, for soil map units dissolved by their dominant grouping.';

COMMENT ON COLUMN {schema}.ecogrouppolygon.ecogroup IS
'Unique alphanumeric identifier of the dominant ecogroup, or standardized ecological-site identifier where no ecogroup is assigned; unmapped areas use an NA identifier containing the soil survey area symbol.';
COMMENT ON COLUMN {schema}.ecogrouppolygon.groupname IS
'Descriptive name of the dominant ecogroup, or ecological-site name where no ecogroup is assigned.';
COMMENT ON COLUMN {schema}.ecogrouppolygon.grouptype IS
'Type of ecological grouping represented by the polygon.';
COMMENT ON COLUMN {schema}.ecogrouppolygon.area_ha IS
'Geodesic area of the dissolved polygon in hectares.';
COMMENT ON COLUMN {schema}.ecogrouppolygon.ecogrouppct IS
'Area-weighted proportion of the polygon made up of the dominant ecogroup or ecological site, calculated from map-unit area and component group percentages and expressed from 0 to 1.';
COMMENT ON COLUMN {schema}.ecogrouppolygon.geom IS
'Validated dissolved multipolygon geometry of the dominant ecogroup or ecological site.';

COMMENT ON TABLE {schema}.ecopolygon IS
'Represents the dominant NRCS ecological site for soil map units dissolved by their dominant ecological-site identifier.';

COMMENT ON COLUMN {schema}.ecopolygon.ecoclassid_std IS
'NRCS alphanumeric identifier of the dominant ecological site, standardized by removing a leading forest or range type code; unmapped areas use an NA identifier containing the soil survey area symbol.';
COMMENT ON COLUMN {schema}.ecopolygon.ecoclassname IS
'NRCS Ecological Classification Name of the dominant ecological site.';
COMMENT ON COLUMN {schema}.ecopolygon.ecotype IS
'Type of ecological site inferred from a leading F or R character in its source Ecological Classification ID: forest or range.';
COMMENT ON COLUMN {schema}.ecopolygon.area_ha IS
'Geodesic area of the dissolved polygon in hectares.';
COMMENT ON COLUMN {schema}.ecopolygon.ecopct IS
'Area-weighted proportion of the polygon made up of the dominant ecological site, calculated from map-unit area and component ecological-site percentages and expressed from 0 to 1.';
COMMENT ON COLUMN {schema}.ecopolygon.geom IS
'Validated dissolved multipolygon geometry of the dominant ecological site.';

-- General SSURGO-derived views from create_views.sql

COMMENT ON VIEW {schema}.chorizon_surface IS
'Contains SSURGO horizon records whose representative upper depth is zero, providing the surface horizon records for each component.';

COMMENT ON VIEW {schema}.chorizon_surface_noom IS
'Selects the shallowest non-organic horizon for each component, excluding horizons whose master designation is O.';
COMMENT ON COLUMN {schema}.chorizon_surface_noom.rn IS
'Rank of the non-organic horizon within the component by representative upper depth; this view retains rank 1.';

COMMENT ON VIEW {schema}.chunified_first IS
'Selects one unified soil classification record per horizon, preferring a representative record and then the lowest unified-record key.';

COMMENT ON VIEW {schema}.coecoclass_codominant IS
'Selects one ecological-class record per component, preferring the ecological-class type that occurs most often in the source database.';
COMMENT ON COLUMN {schema}.coecoclass_codominant.ecoclassid_std IS
'Ecological-class identifier standardized by removing a leading forest or range type code and limiting those identifiers to the ecological-site portion.';
COMMENT ON COLUMN {schema}.coecoclass_codominant.ecoclassname_std IS
'Lowercase, trimmed ecological-class name with quotation marks normalized to the text "in.".';

COMMENT ON VIEW {schema}.comonth_flodpondfreqcl IS
'Summarizes the maximum and most frequently reported flooding and ponding frequency and duration classes for each soil component across its monthly records.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.flodfreqcl_max IS 'Highest-severity flooding frequency class reported for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.flodfreqcl_dom IS 'Most frequently reported flooding frequency class for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.flodfreqcl_dom_n IS 'Number of monthly records in the dominant flooding frequency class.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.floddurcl_max IS 'Longest flooding duration class reported for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.floddurcl_dom IS 'Most frequently reported flooding duration class for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.floddurcl_dom_n IS 'Number of monthly records in the dominant flooding duration class.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.pondfreqcl_max IS 'Highest-severity ponding frequency class reported for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.pondfreqcl_dom IS 'Most frequently reported ponding frequency class for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.pondfreqcl_dom_n IS 'Number of monthly records in the dominant ponding frequency class.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.ponddurcl_max IS 'Longest ponding duration class reported for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.ponddurcl_dom IS 'Most frequently reported ponding duration class for the component.';
COMMENT ON COLUMN {schema}.comonth_flodpondfreqcl.ponddurcl_dom_n IS 'Number of monthly records in the dominant ponding duration class.';

COMMENT ON VIEW {schema}.comonth_soimoistdept IS
'Reports the minimum and maximum representative depth at which wet soil-moisture conditions are recorded for each component.';
COMMENT ON COLUMN {schema}.comonth_soimoistdept.soimoistdept_rmin IS 'Minimum representative depth at which wet soil-moisture conditions are recorded for the component.';
COMMENT ON COLUMN {schema}.comonth_soimoistdept.soimoistdept_rmax IS 'Maximum representative depth at which wet soil-moisture conditions are recorded for the component.';

COMMENT ON VIEW {schema}.copmgrp_first IS
'Selects one parent-material group per component, preferring a representative record and then the lowest parent-material-group key.';

COMMENT ON VIEW {schema}.texture_first IS
'Selects one representative texture group and its first texture record for each soil horizon.';

COMMENT ON VIEW {schema}.coecoclass_mapunit_ranked IS
'Aggregates component percentages by ecological class within each map unit and ranks ecological classes from greatest to least representative percentage.';
COMMENT ON COLUMN {schema}.coecoclass_mapunit_ranked.ecoclassid_std IS 'Ecological-class identifier standardized by removing a leading forest or range type code.';
COMMENT ON COLUMN {schema}.coecoclass_mapunit_ranked.ecoclassname_std IS 'Lowercase, trimmed ecological-class name with quotation marks normalized to text.';
COMMENT ON COLUMN {schema}.coecoclass_mapunit_ranked.ecotype IS 'Ecological-class type inferred from the identifier prefix: forest or range.';
COMMENT ON COLUMN {schema}.coecoclass_mapunit_ranked.ecoclasspct IS 'Sum of representative component percentages assigned to the ecological class within the map unit.';
COMMENT ON COLUMN {schema}.coecoclass_mapunit_ranked.ecorank IS 'Rank of the ecological class within the map unit by representative percentage, with 1 denoting the largest percentage.';

COMMENT ON VIEW {schema}.coecoclass_mudominant IS
'Returns the highest-ranked ecological class for each map unit based on summed representative component percentage.';

COMMENT ON VIEW {schema}.coecoclass_wide IS
'Pivots the six highest-ranked ecological classes for each map unit into a single wide record.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassid_1 IS 'Identifier of the first-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassname_1 IS 'Name of the first-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclasspct_1 IS 'Representative percentage of the first-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassid_2 IS 'Identifier of the second-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassname_2 IS 'Name of the second-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclasspct_2 IS 'Representative percentage of the second-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassid_3 IS 'Identifier of the third-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassname_3 IS 'Name of the third-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclasspct_3 IS 'Representative percentage of the third-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassid_4 IS 'Identifier of the fourth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassname_4 IS 'Name of the fourth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclasspct_4 IS 'Representative percentage of the fourth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassid_5 IS 'Identifier of the fifth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassname_5 IS 'Name of the fifth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclasspct_5 IS 'Representative percentage of the fifth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassid_6 IS 'Identifier of the sixth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclassname_6 IS 'Name of the sixth-ranked ecological class.';
COMMENT ON COLUMN {schema}.coecoclass_wide.ecoclasspct_6 IS 'Representative percentage of the sixth-ranked ecological class.';

COMMENT ON VIEW {schema}.coecoclass_unique IS
'Lists standardized ecological classes occurring in the database and summarizes their map-unit occurrence, mean representative percentage, and estimated mapped area.';
COMMENT ON COLUMN {schema}.coecoclass_unique.eco_n IS 'Number of map units in which the standardized ecological class occurs.';
COMMENT ON COLUMN {schema}.coecoclass_unique.ecoclasspct_mean IS 'Mean representative percentage of the ecological class across map units in which it occurs.';
COMMENT ON COLUMN {schema}.coecoclass_unique.eco_ha IS 'Estimated hectares assigned to the ecological class, calculated from map-unit polygon area and representative component percentage.';

COMMENT ON VIEW {schema}.component_plantprod IS
'Combines range, forest-understory, and normalized forest-overstory plant production into one plant-production list for each soil component.';
COMMENT ON COLUMN {schema}.component_plantprod.prodtype IS 'Plant-production category: range, forest understory, or forest.';
COMMENT ON COLUMN {schema}.component_plantprod.prod IS 'Plant production percentage for the component and production category; forest values are normalized across forest species for the component.';

COMMENT ON VIEW {schema}.coecoclass_plantprod IS
'Estimates component-area-weighted plant production percentages by standardized ecological class, plant, and production category.';
COMMENT ON COLUMN {schema}.coecoclass_plantprod.prodtype IS 'Plant-production category: range, forest understory, or forest.';
COMMENT ON COLUMN {schema}.coecoclass_plantprod.prod IS 'Component-area-weighted mean plant production percentage for the ecological class and production category.';

COMMENT ON VIEW {schema}.component_mapunit_ranked IS
'Ranks components within each map unit by representative percentage and constructs an expanded component name from local phase, surface texture, and slope range.';
COMMENT ON COLUMN {schema}.component_mapunit_ranked.compnamelong IS 'Expanded component name composed from component name, local phase, surface texture description, and slope range when available.';
COMMENT ON COLUMN {schema}.component_mapunit_ranked.comprank IS 'Rank of the component within the map unit by representative percentage, with 1 denoting the largest percentage.';

COMMENT ON VIEW {schema}.component_dominant IS
'Returns the highest-ranked component for each map unit based on representative component percentage.';

COMMENT ON VIEW {schema}.component_wide IS
'Pivots the six highest-ranked components for each map unit into a single wide record, including expanded component names and representative percentages.';
COMMENT ON COLUMN {schema}.component_wide.compnamelong_1 IS 'Expanded name of the first-ranked component.';
COMMENT ON COLUMN {schema}.component_wide.compnamelong_2 IS 'Expanded name of the second-ranked component.';
COMMENT ON COLUMN {schema}.component_wide.compnamelong_3 IS 'Expanded name of the third-ranked component.';
COMMENT ON COLUMN {schema}.component_wide.compnamelong_4 IS 'Expanded name of the fourth-ranked component.';
COMMENT ON COLUMN {schema}.component_wide.compnamelong_5 IS 'Expanded name of the fifth-ranked component.';
COMMENT ON COLUMN {schema}.component_wide.compnamelong_6 IS 'Expanded name of the sixth-ranked component.';

COMMENT ON VIEW {schema}.component_unique IS
'Lists component names occurring in the database and summarizes their component kind, mean representative percentage, and estimated mapped area.';
COMMENT ON COLUMN {schema}.component_unique.comppct_r_mean IS 'Mean representative component percentage across map units in which the component name occurs.';
COMMENT ON COLUMN {schema}.component_unique.comp_ha IS 'Estimated hectares assigned to the component name, calculated from map-unit polygon area and representative component percentage.';

COMMENT ON VIEW {schema}.coecoclass_area IS
'Combines map-unit polygons with ranked ecological classes and estimates the hectares represented by each ecological class in each polygon.';
COMMENT ON COLUMN {schema}.coecoclass_area.eco_ha IS 'Estimated hectares of the map-unit polygon represented by the ecological class.';

COMMENT ON VIEW {schema}.coeplants_gh IS
'Provides a lookup from generic SSURGO plant symbols to duration, growth-habit class, and descriptive name.';
COMMENT ON COLUMN {schema}.coeplants_gh.code IS 'Generic SSURGO plant symbol.';
COMMENT ON COLUMN {schema}.coeplants_gh.dur IS 'Plant duration associated with the generic symbol, when applicable.';
COMMENT ON COLUMN {schema}.coeplants_gh.gh IS 'Growth-habit class associated with the generic symbol, when applicable.';
COMMENT ON COLUMN {schema}.coeplants_gh.name IS 'Descriptive name of the generic plant group.';

COMMENT ON VIEW {schema}.cotaxmoistcl_first IS
'Selects one taxonomic moisture-class record per component using the lowest taxonomic moisture-class key.';
COMMENT ON COLUMN {schema}.cotaxmoistcl_first.rn IS 'Rank of the taxonomic moisture-class record within the component by key; this view retains rank 1.';
