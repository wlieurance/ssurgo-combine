/* This query returns the surface soil horizon and should be joined 
to the component or qry_component_dominant via the cokey in arcGIS */
CREATE OR REPLACE VIEW {schema}.chorizon_surface AS
SELECT * 
  FROM {schema}.chorizon 
 WHERE hzdept_r = 0;


/* This selects the top layer FROM chorizon that is not organic material */
CREATE OR REPLACE VIEW {schema}.chorizon_surface_noOM AS
WITH no_om AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY hzdept_r) AS rn 
  FROM {schema}.chorizon 
 WHERE desgnmaster != 'O'
)

SELECT * FROM no_om WHERE rn = 1;


 /* View isolates only one record in the case of multiple records per soil horizon
join to chorizon or qry_chorizon_surface via chkey */
CREATE OR REPLACE VIEW {schema}.chunified_first AS
WITH unified_rn AS (
SELECT *, Row_Number() OVER (PARTITION BY chkey ORDER BY rvindicator DESC, chunifiedkey) AS rn 
  FROM {schema}.chunified
)

SELECT a.unifiedcl, a.rvindicator, a.chkey, a.chunifiedkey 
  FROM unified_rn AS a 
 WHERE rn = 1;


 /* Isolates one ecosite per component by ecoclasstype in the case of multiple ecosites per component 
(in order of ecoclasstype abundance within the database). JOIN to component or qry_component_dominant via cokey. */
CREATE OR REPLACE VIEW {schema}.coecoclass_codominant AS
WITH econame_cnt AS (
SELECT ecoclasstypename, count(cokey) AS n 
  FROM {schema}.coecoclass 
 GROUP BY ecoclasstypename 

), eco_rn AS (
SELECT a.cokey, a.coecoclasskey, b.n, ROW_NUMBER() OVER(PARTITION BY a.cokey ORDER BY b.n DESC) AS rn 
  FROM {schema}.coecoclass AS a 
 INNER JOIN econame_cnt AS b ON a.ecoclasstypename = b.ecoclasstypename 

), eco_dom AS (
SELECT cokey, coecoclasskey 
  FROM eco_rn AS x 
 WHERE rn = 1 
)        

SELECT y.*, 
       CASE WHEN SUBSTRING(ecoclassid, 1,1) IN ('F','R') THEN SUBSTRING(ecoclassid, 2, 10) 
            WHEN SUBSTRING(ecoclassid, 1,1) = '0' THEN SUBSTRING(ecoclassid, 1, 10) 
            ELSE ecoclassid END AS ecoclassid_std, 
       LOWER(LTRIM(RTRIM(REPLACE(REPLACE(ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std 
  FROM {schema}.coecoclass AS y 
 INNER JOIN eco_dom AS z ON y.coecoclasskey = z.coecoclasskey;


 /* Gives dominant and maximum flooding/ponding frequency class and flooding/ponding frequency duration grouped by comonth.
Join to component or qry_component_dominant via cokey. */
CREATE OR REPLACE VIEW {schema}.comonth_flodpondfreqcl AS
WITH flood_freq AS (
SELECT cokey, flodfreqcl, 
       CASE WHEN flodfreqcl IS NULL THEN NULL
            WHEN flodfreqcl = 'None' THEN 1 
            WHEN flodfreqcl = 'Rare' THEN 2 
            WHEN flodfreqcl = 'Occasional' THEN 3 
            WHEN flodfreqcl = 'Frequent' THEN 4 
            ELSE NULL END AS flodfreqclno
  FROM {schema}.comonth

), flood_dur AS (
SELECT cokey, floddurcl,           
       CASE WHEN floddurcl IS NULL THEN NULL
            WHEN floddurcl = 'Extremely brief (0.1 to 4 hours)' THEN 1 
            WHEN floddurcl = 'Very brief (4 to 48 hours)' THEN 2 
            WHEN floddurcl = 'Brief (2 to 7 days)' THEN 3 
            WHEN floddurcl = 'Long (7 to 30 days)' THEN 4 
            WHEN floddurcl = 'Very long (more than 30 days)' THEN 5 
            ELSE NULL END AS floddurclno
  FROM {schema}.comonth

), pond_freq AS (
SELECT cokey, pondfreqcl, 
       CASE WHEN pondfreqcl IS NULL THEN NULL
            WHEN pondfreqcl = 'None' THEN 1 
            WHEN pondfreqcl = 'Rare' THEN 2 
            WHEN pondfreqcl = 'Occasional' THEN 3 
            WHEN pondfreqcl = 'Frequent' THEN 4 
            ELSE NULL END AS pondfreqclno
  FROM {schema}.comonth

), pond_dur AS (
SELECT cokey, ponddurcl,           
       CASE WHEN ponddurcl IS NULL THEN NULL
            WHEN ponddurcl = 'Extremely brief (0.1 to 4 hours)' THEN 1 
            WHEN ponddurcl = 'Very brief (4 to 48 hours)' THEN 2 
            WHEN ponddurcl = 'Brief (2 to 7 days)' THEN 3 
            WHEN ponddurcl = 'Long (7 to 30 days)' THEN 4 
            WHEN ponddurcl = 'Very long (more than 30 days)' THEN 5 
            ELSE NULL END AS ponddurclno
  FROM {schema}.comonth

), flood_freq_rn AS (
SELECT cokey, flodfreqcl, flodfreqclno, 
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY flodfreqclno DESC) AS rownum
  FROM flood_freq

), flood_dur_rn AS (
SELECT cokey, floddurcl, floddurclno,
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY floddurclno DESC) AS rownum
  FROM flood_dur

), flood_freq_cnt AS (
SELECT cokey, flodfreqcl, count(flodfreqcl) AS flodfreqcl_n
  FROM {schema}.comonth
 GROUP BY cokey, flodfreqcl

), flood_freq_cnt_rn AS (
SELECT cokey, flodfreqcl, flodfreqcl_n,
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY flodfreqcl_n DESC) AS rownum
  FROM flood_freq_cnt

), flood_dur_cnt AS (
SELECT cokey, floddurcl, count(floddurcl) AS floddurcl_n    
  FROM {schema}.comonth
 GROUP BY cokey, floddurcl

), flood_dur_cnt_rn AS (
SELECT cokey, floddurcl, floddurcl_n,
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY floddurcl_n DESC) AS rownum
  FROM flood_dur_cnt

), pond_freq_rn AS (
SELECT cokey, pondfreqcl, pondfreqclno, 
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY pondfreqclno DESC) AS rownum
  FROM pond_freq    

), pond_dur_rn AS (
SELECT cokey, ponddurcl, ponddurclno,
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY ponddurclno DESC) AS rownum
  FROM pond_dur

), pond_freq_cnt AS (
SELECT cokey, pondfreqcl, count(pondfreqcl) AS pondfreqcl_n
  FROM {schema}.comonth
 GROUP BY cokey, pondfreqcl

), pond_freq_cnt_rn AS (
SELECT cokey, pondfreqcl, pondfreqcl_n,
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY pondfreqcl_n DESC) AS rownum
  FROM pond_freq_cnt

), pond_dur_cnt AS (
SELECT cokey, ponddurcl, count(ponddurcl) AS ponddurcl_n    
  FROM {schema}.comonth
 GROUP BY cokey, ponddurcl

), pond_dur_cnt_rn AS (
SELECT cokey, ponddurcl, ponddurcl_n,
       ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY ponddurcl_n DESC) AS rownum
  FROM pond_dur_cnt
)

SELECT a.cokey, 
       b.flodfreqcl AS flodfreqcl_max, d.flodfreqcl AS flodfreqcl_dom, d.flodfreqcl_n AS flodfreqcl_dom_n,
       c.floddurcl AS floddurcl_max, e.floddurcl AS floddurcl_dom, e.floddurcl_n AS floddurcl_dom_n,
       f.pondfreqcl AS pondfreqcl_max, h.pondfreqcl AS pondfreqcl_dom, h.pondfreqcl_n AS pondfreqcl_dom_n,
       g.ponddurcl AS ponddurcl_max, i.ponddurcl AS ponddurcl_dom, i.ponddurcl_n AS ponddurcl_dom_n
  FROM (SELECT cokey FROM {schema}.comonth GROUP BY cokey) AS a
  LEFT JOIN flood_freq_rn b ON a.cokey = b.cokey
  LEFT JOIN flood_dur_rn c ON a.cokey = c.cokey
  LEFT JOIN flood_freq_cnt_rn d ON a.cokey = d.cokey
  LEFT JOIN flood_dur_cnt_rn e ON a.cokey = e.cokey
  LEFT JOIN pond_freq_rn f ON a.cokey = f.cokey
  LEFT JOIN pond_dur_rn g ON a.cokey = g.cokey
  LEFT JOIN pond_freq_cnt_rn h ON a.cokey = h.cokey 
  LEFT JOIN pond_dur_cnt_rn i ON a.cokey = i.cokey
 WHERE b.rownum = 1 AND c.rownum = 1 AND d.rownum = 1 AND e.rownum = 1 
   AND f.rownum = 1 AND g.rownum = 1 AND h.rownum = 1 AND i.rownum = 1;


/* Provides minimum and maximum soil moisture depth levels group by comonth.  JOIN to component or qry_component_dominant */
CREATE OR REPLACE VIEW {schema}.comonth_soimoistdept AS
SELECT a.cokey, min(b.soimoistdept_r) AS soimoistdept_rmin, max(b.soimoistdept_r) AS soimoistdept_rmax 
  FROM {schema}.comonth AS a 
 INNER JOIN {schema}.cosoilmoist AS b ON a.comonthkey = b.comonthkey 
 WHERE soimoiststat = 'Wet' 
 GROUP BY a.cokey;

 /* In the case that there is more than one parent material group per component, returns the one with the alphabetically lowest copmgrpkey.
 where rvindicator = Yes. Join to component via cokey. */
CREATE OR REPLACE VIEW soil.copmgrp_first AS
WITH pm_base AS (
SELECT pmgroupname, rvindicator, cokey, copmgrpkey, 
       row_number() over(partition by cokey ORDER BY rvindicator DESC, copmgrpkey) rn
  FROM soil.copmgrp
)

SELECT pmgroupname, rvindicator, cokey, copmgrpkey 
  FROM pm_base WHERE rn = 1;

 /* In the case that there is more than one texture available per horizon, returns the one with the alphabetically lowest chkey.
Join to chorizon or qry_chorizon_surface via chkey. */
CREATE OR REPLACE VIEW {schema}.texture_first AS
WITH texgrp_min AS (
SELECT chkey, min(chtgkey) AS chtgkey 
  FROM {schema}.chtexturegrp 
 WHERE rvindicator = 'Yes'
 GROUP BY chkey

), texture_min AS (
SELECT chtgkey, min(chtkey) AS chtkey 
  FROM {schema}.chtexture
 GROUP BY chtgkey

), texture_full AS (
SELECT x.* 
  FROM {schema}.chtexture AS x
 INNER JOIN texture_min AS y ON x.chtkey = y.chtkey
)

SELECT a.*, c.texcl, c.lieutex, c.chtkey 
  FROM {schema}.chtexturegrp AS a
 INNER JOIN texgrp_min AS b ON a.chtgkey = b.chtgkey
 INNER JOIN texture_full AS c ON b.chtgkey = c.chtgkey;


/* Creates a list of ecosites per map unit key ranked by total area */
CREATE OR REPLACE VIEW {schema}.coecoclass_mapunit_ranked AS
WITH ecotype_cnt AS (
SELECT ecoclasstypename, Count(cokey) AS n
  FROM {schema}.coecoclass
 GROUP BY ecoclasstypename

), ecotype_rn AS (
SELECT a.cokey, a.coecoclasskey, b.n, 
       ROW_NUMBER() OVER (PARTITION BY a.cokey ORDER BY b.n DESC) AS rn
  FROM {schema}.coecoclass AS a 
 INNER JOIN ecotype_cnt AS b ON a.ecoclasstypename = b.ecoclasstypename

), ecotype_dom AS (
SELECT cokey, coecoclasskey
  FROM ecotype_rn AS x
 WHERE rn = 1

), eco_joined AS (
SELECT y.*
  FROM {schema}.coecoclass AS y 
 INNER JOIN ecotype_dom AS z ON y.coecoclasskey = z.coecoclasskey

), ecoclass_cnt AS (
SELECT ecoclassid, ecoclassname, count(ecoclassname) AS n
  FROM {schema}.coecoclass
 GROUP BY ecoclassid, ecoclassname

), eco_sum AS (
SELECT q.mukey, r.ecoclassid, sum(q.comppct_r) AS ecoclasspct
  FROM {schema}.component AS q
  LEFT JOIN eco_joined AS r ON q.cokey = r.cokey
 GROUP BY mukey, ecoclassid

), econame_rn AS (
SELECT ecoclassid, ecoclassname, n, 
        ROW_NUMBER() OVER(PARTITION BY ecoclassid ORDER BY n DESC, ecoclassname) AS rn
   FROM ecoclass_cnt

), econame_dom AS (
SELECT ecoclassid, 
       CASE WHEN SUBSTRING(ecoclassid, 1, 1) IN ('F', 'R') THEN SUBSTRING(ecoclassid, 2, 10) 
            WHEN SUBSTRING(ecoclassid, 1, 1) = '0' THEN SUBSTRING(ecoclassid, 1, 10) 
            ELSE ecoclassid END AS ecoclassid_std,
       CASE WHEN SUBSTRING(ecoclassid, 1, 1) = 'F' THEN 'forest'
            WHEN SUBSTRING(ecoclassid, 1, 1) = 'R' THEN 'range'
            ELSE NULL END AS ecotype,    
       ecoclassname 
  FROM econame_rn AS k
 WHERE rn = 1

), ecorank AS (
SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, l.ecotype, g.ecoclasspct, 
       ROW_NUMBER() OVER(PARTITION BY g.mukey ORDER BY g.ecoclasspct Desc, g.ecoclassid) AS ecorank
  FROM eco_sum AS g
  LEFT JOIN econame_dom AS l ON g.ecoclassid = l.ecoclassid
)

SELECT mukey, ecoclassid, ecoclassid_std, ecoclassname, 
       LOWER(LTRIM(RTRIM(REPLACE(REPLACE(ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std, 
       ecotype, ecoclasspct, ecorank
  FROM ecorank;


/* Identifies the dominant ecosite per map unit by summing up comppct_r values*/
CREATE OR REPLACE VIEW {schema}.coecoclass_mudominant AS
SELECT mukey, ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, ecotype, ecoclasspct 
  FROM {schema}.coecoclass_mapunit_ranked
 WHERE ecorank = 1;


/* Isolates the six most dominant ecosites per mapunit and arranges them in a wide table format with their percentages.
Join to mupolygon or mapunit via mukey */
CREATE OR REPLACE VIEW {schema}.coecoclass_wide AS
SELECT a.mukey, 
       n.ecoclassid AS ecoclassid_1, n.ecoclassname AS ecoclassname_1, n.ecoclasspct AS ecoclasspct_1, 
       o.ecoclassid AS ecoclassid_2, o.ecoclassname AS ecoclassname_2, o.ecoclasspct AS ecoclasspct_2, 
       p.ecoclassid AS ecoclassid_3, p.ecoclassname AS ecoclassname_3, p.ecoclasspct AS ecoclasspct_3, 
       q.ecoclassid AS ecoclassid_4, q.ecoclassname AS ecoclassname_4, q.ecoclasspct AS ecoclasspct_4, 
       r.ecoclassid AS ecoclassid_5, r.ecoclassname AS ecoclassname_5, r.ecoclasspct AS ecoclasspct_5, 
       s.ecoclassid AS ecoclassid_6, s.ecoclassname AS ecoclassname_6, s.ecoclasspct AS ecoclasspct_6
  FROM {schema}.mapunit AS a
  LEFT JOIN (SELECT * FROM {schema}.coecoclass_mapunit_ranked WHERE ecorank = 1) AS n ON a.mukey = n.mukey
  LEFT JOIN (SELECT * FROM {schema}.coecoclass_mapunit_ranked WHERE ecorank = 2) AS o ON a.mukey = o.mukey
  LEFT JOIN (SELECT * FROM {schema}.coecoclass_mapunit_ranked WHERE ecorank = 3) AS p ON a.mukey = p.mukey
  LEFT JOIN (SELECT * FROM {schema}.coecoclass_mapunit_ranked WHERE ecorank = 4) AS q ON a.mukey = q.mukey
  LEFT JOIN (SELECT * FROM {schema}.coecoclass_mapunit_ranked WHERE ecorank = 5) AS r ON a.mukey = r.mukey
  LEFT JOIN (SELECT * FROM {schema}.coecoclass_mapunit_ranked WHERE ecorank = 6) AS s ON a.mukey = s.mukey;


/* Creates a unique list of ecosites in the database and calculates area statistics via mupolygon.geom and component.comppct_r (eco_ha).
ecoclasspct_mean is the average percentage of the a map unit the ecosite takes up if it is present. */
CREATE OR REPLACE VIEW {schema}.coecoclass_unique AS
WITH mu_area_sum AS (
SELECT mukey, sum(area_ha) AS area_ha
  FROM {schema}.mupolygon
 GROUP BY mukey

), eco_area_pct AS (
SELECT a.mukey, a.ecoclassid, a.ecoclassid_std, a.ecoclassname, a.ecoclassname_std, 
       a.ecotype, a.ecoclasspct, a.ecorank, (a.ecoclasspct * b.area_ha / 100) AS eco_ha
  FROM {schema}.coecoclass_mapunit_ranked AS a
  LEFT JOIN mu_area_sum AS b ON a.mukey = b.mukey

), eco_area_mean AS (
SELECT x.ecoclassid_std, COUNT(x.mukey) AS eco_n,
       AVG(CAST(ecoclasspct AS REAL)) AS ecoclasspct_mean, SUM(eco_ha) AS eco_ha
  FROM eco_area_pct AS x
 GROUP BY x.ecoclassid_std

), ecotype_cnt AS (
SELECT ecoclasstypename, count(ecoclasstypename) AS n 
  FROM {schema}.coecoclass 
 GROUP BY ecoclasstypename

), ecotype_rn AS (
SELECT a.ecoclasstypename, a.n, ROW_NUMBER() OVER(ORDER BY a.n DESC) AS pref_order 
  FROM ecotype_cnt AS a

), ecotype_join AS (
SELECT x.ecoclasstypename, x.ecoclassid, 
       CASE WHEN Substring(ecoclassid,1,1) = '0' THEN Substring(ecoclassid,1,10)
            WHEN Substring(ecoclassid,1,1) IN ('R','F') THEN Substring(ecoclassid,2,10)
            ELSE NULL END AS ecoclassid_std,
       x.ecoclassname, y.pref_order
  FROM {schema}.coecoclass AS x
 INNER JOIN ecotype_rn AS y ON x.ecoclasstypename = y.ecoclasstypename

), ecotype_grp AS (
SELECT f.ecoclasstypename, f.ecoclassid_std, f.ecoclassid, f.ecoclassname, f.pref_order
  FROM ecotype_join AS f
 GROUP BY f.ecoclasstypename, f.ecoclassid_std, f.ecoclassid, f.ecoclassname, f.pref_order

), ecotype_grp_rn AS (
SELECT g.ecoclasstypename, g.ecoclassid_std, g.ecoclassid, g.ecoclassname, g.pref_order,
       ROW_NUMBER() OVER(PARTITION BY g.ecoclassid_std ORDER BY g.pref_order) AS preference
  FROM ecotype_grp AS g

), eco_select AS (
SELECT h.ecoclasstypename, h.ecoclassid_std, h.ecoclassid, h.ecoclassname,
       LOWER(LTRIM(RTRIM(REPLACE(REPLACE(h.ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std 
  FROM ecotype_grp_rn AS h
 WHERE h.preference = 1 AND 
       LOWER(h.ecoclassid) != 'null' AND 
       h.ecoclassid IS NOT NULL AND
       POSITION('?' IN h.ecoclassid) = 0
)

SELECT i.ecoclassid_std, j.ecoclassid, j.ecoclassname, j.ecoclassname_std, 
       j.ecoclasstypename, i.eco_n, i.ecoclasspct_mean, i.eco_ha
  FROM eco_area_mean i
  LEFT JOIN eco_select AS j ON i.ecoclassid_std = j.ecoclassid_std;


/* This view will create a list of unique plants and their % production values by component*/
CREATE OR REPLACE VIEW {schema}.component_plantprod AS
WITH forest_species AS (
SELECT plantsym, plantsciname, plantcomname, fprod_r, cokey, cofprodkey
  FROM {schema}.coforprod
 WHERE fprod_r > 0 

), forest_all AS (
SELECT cokey, sum(fprod_r) fprod_sum
  FROM {schema}.coforprod
 WHERE fprod_r IS NOT NULL
 GROUP BY cokey
HAVING sum(fprod_r) > 0

), forest_species_calc AS (
SELECT a.*, b.fprod_sum, CAST(round((CAST(a.fprod_r AS NUMERIC)/CAST(b.fprod_sum AS NUMERIC) * 100), 0) AS INTEGER) forestprod 
  FROM forest_species a 
 INNER JOIN forest_all b ON a.cokey = b.cokey

), base AS (
SELECT d.cokey,
       d.plantsym, d.plantsciname, d.plantcomname, d.rangeprod AS prod, 'range' AS prodtype
  FROM {schema}.coeplants AS d
 WHERE d.rangeprod IS NOT NULL
 UNION
SELECT d.cokey,
       d.plantsym, d.plantsciname, d.plantcomname, d.forestunprod AS prod, 'forest understory' AS prodtype
  FROM {schema}.coeplants AS d
 WHERE d.forestunprod IS NOT NULL
 UNION
SELECT d.cokey,
       d.plantsym, d.plantsciname, d.plantcomname, d.forestprod AS prod, 'forest' AS prodtype
  FROM forest_species_calc AS d
 WHERE d.forestprod IS NOT NULL
)

SELECT cokey, plantsym, plantsciname, plantcomname, prodtype, prod 
  FROM base;


/* This view will create a list of unique plants and their % production values, weighted by component area within map units */
CREATE OR REPLACE VIEW {schema}.coecoclass_plantprod AS
WITH forest_species AS (
SELECT plantsym, plantsciname, plantcomname, fprod_r, cokey, cofprodkey
  FROM {schema}.coforprod
 WHERE fprod_r > 0 

), forest_all AS (
SELECT cokey, sum(fprod_r) fprod_sum
  FROM {schema}.coforprod
 WHERE fprod_r IS NOT NULL
 GROUP BY cokey
HAVING sum(fprod_r) > 0

), forest_species_calc AS (
SELECT a.*, b.fprod_sum, CAST(round((CAST(a.fprod_r AS NUMERIC)/CAST(b.fprod_sum AS NUMERIC) * 100), 0) AS INTEGER) forestprod 
  FROM forest_species a 
 INNER JOIN forest_all b ON a.cokey = b.cokey

), base AS (
SELECT c.cokey,  c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, 
       d.plantsym, d.plantsciname, d.plantcomname, d.rangeprod AS prod, 'range' AS prodtype
  FROM {schema}.coecoclass_codominant AS c
 INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
 WHERE d.rangeprod IS NOT NULL
 UNION
SELECT c.cokey,  c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, 
       d.plantsym, d.plantsciname, d.plantcomname, d.forestunprod AS prod, 'forest understory' AS prodtype
  FROM {schema}.coecoclass_codominant AS c
 INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
 WHERE d.forestunprod IS NOT NULL
 UNION
SELECT c.cokey,  c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, 
       d.plantsym, d.plantsciname, d.plantcomname, d.forestprod AS prod, 'forest' AS prodtype
  FROM {schema}.coecoclass_codominant AS c
 INNER JOIN forest_species_calc AS d ON c.cokey = d.cokey
 WHERE d.forestprod IS NOT NULL

), base_unique_eco AS (
SELECT ecoclassid_std, MIN(ecoclassid) AS ecoclassid, MIN(ecoclassname_std) AS ecoclassname_std, 
       plantsym, MIN(plantsciname) AS plantsciname, MIN(plantcomname) AS plantcomname, prodtype
  FROM base
 GROUP BY ecoclassid_std, plantsym, prodtype
    
), base_prod_avg AS (
SELECT e.cokey, e.ecoclassid_std, e.plantsym, e.prodtype, AVG(e.prod) AS prod
  FROM base AS e
 GROUP BY e.cokey, e.ecoclassid_std, e.plantsym, e.prodtype

), base_eco_join AS (
SELECT r.cokey, r.ecoclassid_std, r.ecoclassid, r.ecoclassname_std, q.plantsym, q.plantsciname, q.plantcomname, q.prodtype
  FROM {schema}.coecoclass_codominant AS r
  LEFT JOIN base_unique_eco AS q ON r.ecoclassid_std = q.ecoclassid_std

), base_soil_join AS (
SELECT f.mukey, f.area_ha, g.cokey, g.comppct_r, h.ecoclassid_std, h.ecoclassid, h.ecoclassname_std, h.plantsym, h.plantsciname, h.plantcomname, h.prodtype,
       (CAST(f.area_ha AS REAL) * g.comppct_r / 100) AS comp_ha, 
       COALESCE(i.prod, 0) AS prod
  FROM {schema}.mupolygon AS f
  INNER JOIN {schema}.component AS g ON f.mukey = g.mukey
  INNER JOIN base_eco_join AS h ON g.cokey = h.cokey
  LEFT JOIN base_prod_avg AS i ON h.cokey = i.cokey
        AND h.ecoclassid_std = i.ecoclassid_std
        AND h.plantsym = i.plantsym
        AND h.prodtype = i.prodtype

), prod_wgt AS (
SELECT w.ecoclassid_std, MIN(w.ecoclassid) AS ecoclassid, MIN(w.ecoclassname_std) AS ecoclassname_std, w.plantsym, 
       MIN(w.plantsciname) AS plantsciname, MIN(w.plantcomname) AS plantcomname, w.prodtype, 
       SUM(w.comp_ha * w.prod) AS prodwgt, SUM(w.comp_ha) AS comp_ha
  FROM base_soil_join AS w
 GROUP BY w.ecoclassid_std, w.plantsym, w.prodtype

), prod_wgt_sum AS (
SELECT v.ecoclassid_std, v.ecoclassid, v.ecoclassname_std, v.plantsym, v.plantsciname, v.plantcomname, v.prodtype, 
       CASE WHEN (v.prodwgt IS NULL OR v.comp_ha IS NULL OR v.comp_ha = 0) THEN 0 
            ELSE ROUND(CAST(v.prodwgt/v.comp_ha AS NUMERIC), 1) END AS prod
  FROM prod_wgt AS v
)

SELECT * FROM prod_wgt_sum;

/* Creates a list of components per map unit key ranked by total area */
CREATE OR REPLACE VIEW {schema}.component_mapunit_ranked AS
WITH texgrp_min AS (
SELECT chkey, min(chtgkey) AS chtgkey
  FROM {schema}.chtexturegrp
 GROUP BY chkey

), texgrp_full AS (
SELECT x.* 
  FROM {schema}.chtexturegrp AS x
 INNER JOIN texgrp_min AS y ON x.chtgkey = y.chtgkey

), comp_rn AS (
SELECT mukey, cokey, compname, comppct_r, localphase, slope_l, slope_h, 
       ROW_NUMBER() OVER (PARTITION BY mukey ORDER BY comppct_r DESC, compname ASC) AS comprank
  FROM {schema}.component

), horizon_first AS (
SELECT * FROM {schema}.chorizon WHERE hzdept_r = 0
)

SELECT a.mukey, a.cokey, a.compname, 
       a.compname ||
       CASE WHEN a.localphase IS NULL THEN '' 
            ELSE ' ' || a.localphase END ||
       CASE WHEN c.texdesc IS NULL THEN '' 
            ELSE ' ' || LOWER(c.texdesc) END ||
       CASE WHEN a.slope_l IS NULL THEN '' 
            ELSE ', ' ||
              CAST(a.slope_l AS INTEGER) ||
              ' to ' ||
              CAST(a.slope_h AS INTEGER) ||
              '% slope'
            END 
       AS compnamelong, 
       a.comppct_r, a.comprank 
  FROM comp_rn AS a
  LEFT JOIN horizon_first as b ON a.cokey = b.cokey
  LEFT JOIN texgrp_full AS c ON b.chkey = c.chkey;


/* Provides the most dominant component per map unit by comppct_r and descending alphabetical order.
Join to mupolygon or mapunit via mukey. */
CREATE OR REPLACE VIEW {schema}.component_dominant AS
SELECT mukey, cokey, compname, compnamelong, comppct_r
  FROM {schema}.component_mapunit_ranked AS a 
 WHERE comprank = 1;


/* Isolates the six most dominant components per mapunit and arranges them in a wide table format with their percentages
Also renames components by adding local phase, surface texture, and slope range.  JOIN to mupolygon or mapunit via mukey */
CREATE OR REPLACE VIEW {schema}.component_wide AS
SELECT x.mukey,
       q.cokey AS cokey_1, q.compname AS compname_1, q.compnamelong AS compnamelong_1, q.comppct_r AS comppct_r_1, 
       r.cokey AS cokey_2, r.compname AS compname_2, r.compnamelong AS compnamelong_2, r.comppct_r AS comppct_r_2, 
       s.cokey AS cokey_3, s.compname AS compname_3, s.compnamelong AS compnamelong_3, s.comppct_r AS comppct_r_3, 
       t.cokey AS cokey_4, t.compname AS compname_4, t.compnamelong AS compnamelong_4, t.comppct_r AS comppct_r_4, 
       u.cokey AS cokey_5, u.compname AS compname_5, u.compnamelong AS compnamelong_5, u.comppct_r AS comppct_r_5, 
       v.cokey AS cokey_6, v.compname AS compname_6, v.compnamelong AS compnamelong_6, v.comppct_r AS comppct_r_6 
  FROM {schema}.mapunit AS x
  LEFT JOIN (SELECT * FROM {schema}.component_mapunit_ranked WHERE comprank = 1) AS q ON x.mukey = q.mukey
  LEFT JOIN (SELECT * FROM {schema}.component_mapunit_ranked WHERE comprank = 2) AS r ON x.mukey = r.mukey
  LEFT JOIN (SELECT * FROM {schema}.component_mapunit_ranked WHERE comprank = 3) AS s ON x.mukey = s.mukey
  LEFT JOIN (SELECT * FROM {schema}.component_mapunit_ranked WHERE comprank = 4) AS t ON x.mukey = t.mukey
  LEFT JOIN (SELECT * FROM {schema}.component_mapunit_ranked WHERE comprank = 5) AS u ON x.mukey = u.mukey
  LEFT JOIN (SELECT * FROM {schema}.component_mapunit_ranked WHERE comprank = 6) AS v ON x.mukey = v.mukey;


/* Compiles a list of unique components in the database and displays some statistics, including total area in hectares
Calculated FROM the mupolygon feature and the comppct_r field. */
CREATE OR REPLACE VIEW {schema}.component_unique AS
WITH mu_area_sum AS (
SELECT mukey, sum(area_ha) AS area_ha
  FROM {schema}.mupolygon
 GROUP BY mukey

), comp_pct AS (
SELECT a.mukey, a.compname, a.compkind, a.comppct_r,
      (a.comppct_r * b.area_ha / 100) AS comp_ha
  FROM {schema}.component AS a
  LEFT JOIN mu_area_sum AS b ON a. mukey = b.mukey
)

SELECT x.compname, min(x.compkind) AS compkind, 
       avg(CAST(x.comppct_r AS REAL)) AS comppct_r_mean, sum(x.comp_ha) AS comp_ha
  FROM comp_pct AS x
 GROUP BY x.compname;


/* Calculates the hectares of each ecogroup for each map unit polygon.*/
CREATE OR REPLACE VIEW {schema}.coecoclass_area AS
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha,
       b.ecoclassid, b.ecoclassid_std, b.ecoclassname, b.ecoclassname_std,
       b.ecotype, b.ecoclasspct, b.ecorank,
       a.area_ha * b.ecoclasspct/100 AS eco_ha
  FROM {schema}.mupolygon a
  LEFT JOIN {schema}.coecoclass_mapunit_ranked b ON a.mukey = b.mukey
 ORDER BY a.areasymbol, a.musym, b.ecorank;


CREATE OR REPLACE VIEW {schema}.coeplants_gh AS
WITH
generic_gh (code, dur, gh, name) as (VALUES 
    ('2FA', 'Annual', 'Forb/herb', 'annual forbs'),
    ('2FORB', NULL, 'Forb/herb', 'forbs'),
    ('2FP', 'Perennial', 'Forb/herb', 'perennial forbs'),
    ('2GA', 'Annual', 'Graminoid', 'annual grasses'),
    ('2GL', NULL, 'Graminoid', 'grasslikes'),
    ('2GLA', 'Annual', 'Graminoid', 'annual grasslikes'),
    ('2GLP', 'Perennial', 'Graminoid', 'perennial grasslikes'),
    ('2GP', 'Perennial', 'Graminoid', 'perennial grasses'),
    ('2LF', NULL, 'Lichenous', 'foliose lichens'),
    ('2LICHN', NULL, 'Lichenous', 'lichens'),
    ('2LTR', NULL, NULL, 'litter'),
    ('2MOSS', NULL, 'Nonvascular', 'mosses'),
    ('2SD', 'Perennial', 'Shrub', 'deciduous shrubs'),
    ('2SE', 'Perennial', 'Shrub', 'evergreen shrubs'),
    ('2SHRUB', 'Perennial', 'Shrub', 'shrubs'),
    ('2SUBS', 'Perennial', 'Subshrub', 'subshrubs'),
    ('2TD', 'Perennial', 'Tree', 'deciduous trees'),
    ('2TE', 'Perennial', 'Tree', 'evergreen trees'),
    ('2TN', 'Perennial', 'Tree', 'coniferous trees'),
    ('2TREE', 'Perennial', 'Tree', 'trees'))

SELECT * FROM generic_gh;

CREATE VIEW IF NOT EXISTS {schema}.cotaxmoistcl_first AS 
WITH moist_rn AS (
SELECT taxmoistcl, cokey, cotaxmckey, 
       row_number() over(partition by cokey order by cotaxmckey) rn
  FROM {schema}.cotaxmoistcl
)

SELECT * FROM moist_rn WHERE rn = 1;
