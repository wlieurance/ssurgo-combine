/* This view creates a list of ecosites within ecogroups and attaches ecogroup metadata details */
CREATE OR REPLACE VIEW {schema}.ecogroup_detail AS
SELECT a.ecoid, a.ecogroup, b.groupname, b.grouptype,
       CASE WHEN b.ecogroup IS NULL THEN NULL WHEN a.ecoid = b.modal_site THEN 1 ELSE 0 END AS modal
  FROM {schema}.ecogroup AS a
  LEFT JOIN {schema}.ecogroup_meta AS b ON a.ecogroup = b.ecogroup;

/* This  view ranks ecogroup per map unit by area percent from values listed in the ecogroup table. */
CREATE OR REPLACE VIEW {schema}.ecogroup_mapunit_ranked AS
WITH ecotype_cnt AS (
SELECT ecoclasstypename, count(cokey) AS n
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

), ecoclass_filt AS (
SELECT y.*
  FROM {schema}.coecoclass AS y 
 INNER JOIN ecotype_dom AS z ON y.coecoclasskey = z.coecoclasskey

), ecoclass_cnt AS (
SELECT ecoclassid, ecoclassname, count(ecoclassname) AS n
  FROM {schema}.coecoclass
 GROUP BY ecoclassid, ecoclassname

), ecoclass_rn AS (
SELECT ecoclassid, ecoclassname, n, 
       ROW_NUMBER() OVER(PARTITION BY ecoclassid ORDER BY n DESC, ecoclassname) AS rn
  FROM ecoclass_cnt

), ecoclass_joined AS (
SELECT ecoclassid, 
       CASE WHEN SUBSTRING(ecoclassid, 1, 1) IN ('F', 'R') THEN SUBSTRING(ecoclassid, 2, 10) 
            WHEN SUBSTRING(ecoclassid, 1, 1) = '0' THEN SUBSTRING(ecoclassid, 1, 10) 
            ELSE ecoclassid END AS ecoclassid_std, 
       ecoclassname 
  FROM ecoclass_rn AS k
 WHERE rn = 1

), comp_sum AS (
SELECT q.mukey, r.ecoclassid, Sum(q.comppct_r) AS ecoclasspct
  FROM {schema}.component AS q
  LEFT JOIN ecoclass_filt AS r ON q.cokey = r.cokey
 GROUP BY mukey, ecoclassid

), comp_join AS (
SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, g.ecoclasspct, 
       ROW_NUMBER() OVER(PARTITION BY g.mukey ORDER BY g.ecoclasspct Desc, g.ecoclassid) AS rn
  FROM comp_sum AS g
  LEFT JOIN ecoclass_joined AS l ON g.ecoclassid = l.ecoclassid

), ecogroup_join AS (
SELECT m.mukey, m.ecoclassid, m.ecoclassid_std, m.ecoclassname, 
       CASE WHEN n.ecogroup IS NULL THEN (CASE WHEN m.ecoclassname IS NULL THEN NULL ELSE 
                 LOWER(LTRIM(RTRIM(REPLACE(REPLACE(m.ecoclassname,'"',' in'),'  ',' ')))) END)
            ELSE n.groupname END AS groupname, 
       m.ecoclasspct,
       CASE WHEN m.ecoclassid_std IS NULL THEN NULL
            WHEN n.ecogroup IS NULL THEN m.ecoclassid_std 
            ELSE n.ecogroup END AS ecogroup, 
       CASE WHEN m.ecoclassid_std IS NULL THEN Null
            WHEN n.ecogroup IS NULL THEN (CASE WHEN LEFT(m.ecoclassid, 1) = 'R' THEN 'Range'
                                               WHEN LEFT(m.ecoclassid, 1) = 'F' THEN 'Forest'
                                               ELSE Null END)
            ELSE Null END AS ecosubgroup,
       CASE WHEN m.ecoclassid_std IS NULL THEN 'Ecosite'
            WHEN n.ecogroup IS NULL THEN 'Ecosite'
            ELSE n.grouptype END AS grouptype
  FROM comp_join AS m
  LEFT JOIN {schema}.ecogroup_detail AS n on m.ecoclassid_std = n.ecoid

), ecogroup_area_sum AS (
SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype, sum(a.ecoclasspct) AS ecogrouppct
  FROM ecogroup_join AS a
 GROUP BY mukey, ecogroup, groupname, grouptype

), ecogroup_rn AS (
SELECT Row_Number() OVER (PARTITION BY b.mukey ORDER BY b.ecogrouppct DESC, b.grouptype ASC, b.ecogroup ASC) AS grouprank, 
       b.mukey, b.ecogroup, b.groupname, b.grouptype, b.ecogrouppct
  FROM ecogroup_area_sum AS b

)
SELECT f.mukey, f.ecogroup, f.groupname, f.grouptype, f.ecogrouppct, f.grouprank
  FROM ecogroup_rn AS f;


/* This  view ranks ecogroup per map unit by area percent values listed in the ecogroup table. */
CREATE OR REPLACE VIEW {schema}.ecogroup_mudominant AS
SELECT mukey, ecogroup, groupname, grouptype, ecogrouppct
  FROM {schema}.ecogroup_mapunit_ranked
 WHERE grouprank = 1;


 /*  Creates a list of unique ecogroups and calculates area statistics based on mupolygon.geom and component.comppct_r */
CREATE OR REPLACE VIEW {schema}.ecogroup_unique AS
WITH mu_area_sum AS (
SELECT mukey, sum(area_ha) AS area_ha
    FROM {schema}.mupolygon
GROUP BY mukey

), mu_area_calc AS (
SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype,
       a.ecogrouppct, a.grouprank, (a.ecogrouppct * b.area_ha / 100) AS group_ha
  FROM {schema}.ecogroup_mapunit_ranked AS a
  LEFT JOIN mu_area_sum AS b ON a.mukey = b.mukey
)

SELECT x.ecogroup, COUNT(x.mukey) AS group_n, 
        AVG(CAST(ecogrouppct AS REAL)) AS ecogrouppct_mean, SUM(group_ha) AS group_ha,
        MIN(groupname) AS groupname, MIN(grouptype) AS grouptype
  FROM mu_area_calc AS x
  GROUP BY x.ecogroup;


/* Isolates the six most dominant ecogroups/ecosites per mapunit and arranges them in a wide table format with their percentages
Ecogroup/ecosites percentages summed within map unit.  join to mupolygon or mapunit via mukey. */
CREATE OR REPLACE VIEW {schema}.ecogroup_wide AS
SELECT x.mukey, 
       a.ecogroup AS ecogroup_1, a.groupname AS groupname_1, a.ecogrouppct AS ecogrouppct_1, 
       b.ecogroup AS ecogroup_2, b.groupname AS groupname_2, b.ecogrouppct AS ecogrouppct_2, 
       c.ecogroup AS ecogroup_3, c.groupname AS groupname_3, c.ecogrouppct AS ecogrouppct_3, 
       d.ecogroup AS ecogroup_4, d.groupname AS groupname_4, d.ecogrouppct AS ecogrouppct_4, 
       e.ecogroup AS ecogroup_5, e.groupname AS groupname_5, e.ecogrouppct AS ecogrouppct_5, 
       f.ecogroup AS ecogroup_6, f.groupname AS groupname_6, f.ecogrouppct AS ecogrouppct_6
  FROM {schema}.mapunit AS x
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=1) AS a ON x.mukey = a.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=2) AS b ON x.mukey = b.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=3) AS c ON x.mukey = c.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=4) AS d ON x.mukey = d.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=5) AS e ON x.mukey = e.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=6) AS f ON x.mukey = f.mukey;


/* Calculates the hectares of each ecogroup within a map unit. */
CREATE OR REPLACE VIEW {schema}.ecogroup_area AS
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha,
       b.ecogroup, b.groupname, b.grouptype, b.ecogrouppct, b.grouprank,
       a.area_ha * b.ecogrouppct/100 AS ecogroup_ha
  FROM {schema}.mupolygon a
  LEFT JOIN {schema}.ecogroup_mapunit_ranked b ON a.mukey = b.mukey
 ORDER BY a.areasymbol, a.musym, b.grouprank;


/* Calculates area weighted plant production for each ecogroup. */
CREATE OR REPLACE VIEW {schema}.ecogroup_plantprod AS
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
SELECT a.*, b.fprod_sum, CAST(round((CAST(a.fprod_r AS numeric)/CAST(b.fprod_sum AS numeric) * 100), 0) AS INTEGER) forestprod 
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
SELECT coalesce(q.ecogroup, w.ecoclassid_std) ecogroup, 
	   CASE WHEN q.ecoid IS NULL THEN 'ecosite' ELSE 'ecogroup' END grouptype,
	   w.plantsym, 
       MIN(w.plantsciname) AS plantsciname, MIN(w.plantcomname) AS plantcomname, w.prodtype, 
       SUM(w.comp_ha * w.prod) AS prodwgt, SUM(w.comp_ha) AS comp_ha
  FROM base_soil_join AS w
  LEFT JOIN {schema}.ecogroup q ON w.ecoclassid_std = q.ecoid
 GROUP BY coalesce(q.ecogroup, w.ecoclassid_std), 
	      CASE WHEN q.ecoid IS NULL THEN 'ecosite' ELSE 'ecogroup' END,
	      w.plantsym, w.prodtype

), prod_wgt_sum AS (
SELECT v.ecogroup, v.grouptype, v.plantsym, v.plantsciname, v.plantcomname, v.prodtype, 
       CASE WHEN (v.prodwgt IS NULL OR v.comp_ha IS NULL OR v.comp_ha = 0) THEN 0 
            ELSE ROUND(CAST(v.prodwgt/v.comp_ha AS numeric), 1) END AS prod
  FROM prod_wgt AS v
)

SELECT * FROM prod_wgt_sum;


/* Spatial view showing dominant ecogroup per polygon with area percentage of ecogroup. Inserted into table for usefulness and speed.
{st_direction}: OGC sf = ST_ForcePolygonCCW, ESRI = ST_ForcePolygonCW */
INSERT INTO {schema}.ecogrouppolygon (ecogroup, groupname, grouptype, area_ha, ecogrouppct, geom)
WITH subgroup AS (
SELECT * 
  FROM {schema}.ecogroup_mapunit_ranked 
 WHERE grouprank = 1
    
), group_area_mu AS (
SELECT a.geom, COALESCE(b.ecogroup, 'NA' || '_' || a.areasymbol) AS ecogroup, b.groupname, b.grouptype,
       a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha
  FROM {schema}.mupolygon AS a
  LEFT JOIN subgroup AS b ON a.mukey = b.mukey

), group_union AS (
SELECT ST_Multi(ST_Union(geom)) AS geom, ecogroup, min(groupname) AS groupname, min(grouptype) AS grouptype, 
       sum(ecogrouparea_ha) AS ecogrouparea_ha
  FROM group_area_mu
 GROUP BY ecogroup

), group_cleaned AS (
SELECT ecogroup, groupname, grouptype, ecogrouparea_ha,
       {st_direction}(ST_CollectionExtract(ST_MakeValid(geom), 3)) AS geom
  FROM group_union

), group_area AS (
SELECT ecogroup, groupname, grouptype, ST_Area(geom, True)/10000 AS area_ha, ecogrouparea_ha,
       geom
  FROM group_cleaned
 WHERE ST_IsValid(geom)

), group_pct AS (
SELECT ecogroup, groupname, grouptype, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, ST_Multi(geom) geom
  FROM group_area
)

SELECT * FROM  group_pct;

CREATE INDEX IF NOT EXISTS ecogrouppolygon_gix ON {schema}.ecogrouppolygon USING GIST (geom);
