DROP TABLE IF EXISTS forest_species;
CREATE TEMP TABLE forest_species AS
SELECT plantsym, plantsciname, plantcomname, fprod_r, cokey, cofprodkey
  FROM main.coforprod
 WHERE fprod_r > 0;

DROP TABLE IF EXISTS forest_all;
CREATE TEMP TABLE forest_all AS
SELECT cokey, sum(fprod_r) fprod_sum
  FROM main.coforprod
 WHERE fprod_r IS NOT NULL
 GROUP BY cokey
HAVING sum(fprod_r) > 0;

DROP TABLE IF EXISTS forest_species_calc;
CREATE TEMP TABLE forest_species_calc AS
SELECT a.*, b.fprod_sum, CAST(round((CAST(a.fprod_r AS numeric)/CAST(b.fprod_sum AS numeric) * 100), 0) AS INTEGER) forestprod 
  FROM forest_species a 
 INNER JOIN forest_all b ON a.cokey = b.cokey;

DROP TABLE IF EXISTS base;
CREATE TEMP TABLE base AS
SELECT c.cokey,  c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, 
       d.plantsym, d.plantsciname, d.plantcomname, d.rangeprod AS prod, 'range' AS prodtype
  FROM main.coecoclass_codominant AS c
 INNER JOIN main.coeplants AS d ON c.cokey = d.cokey
 WHERE d.rangeprod IS NOT NULL
 UNION
SELECT c.cokey,  c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, 
       d.plantsym, d.plantsciname, d.plantcomname, d.forestunprod AS prod, 'forest understory' AS prodtype
  FROM main.coecoclass_codominant AS c
 INNER JOIN main.coeplants AS d ON c.cokey = d.cokey
 WHERE d.forestunprod IS NOT NULL
 UNION
SELECT c.cokey,  c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, 
       d.plantsym, d.plantsciname, d.plantcomname, d.forestprod AS prod, 'forest' AS prodtype
  FROM main.coecoclass_codominant AS c
 INNER JOIN forest_species_calc AS d ON c.cokey = d.cokey
 WHERE d.forestprod IS NOT NULL;

DROP TABLE IF EXISTS base_unique_eco;
CREATE TEMP TABLE base_unique_eco AS
SELECT ecoclassid_std, MIN(ecoclassid) AS ecoclassid, MIN(ecoclassname_std) AS ecoclassname_std, 
       plantsym, MIN(plantsciname) AS plantsciname, MIN(plantcomname) AS plantcomname, prodtype
  FROM base
 GROUP BY ecoclassid_std, plantsym, prodtype;

DROP TABLE IF EXISTS base_prod_avg;
CREATE TEMP TABLE base_prod_avg AS
SELECT e.cokey, e.ecoclassid_std, e.plantsym, e.prodtype, AVG(e.prod) AS prod
  FROM base AS e
 GROUP BY e.cokey, e.ecoclassid_std, e.plantsym, e.prodtype;

DROP TABLE IF EXISTS base_eco_join;
CREATE TEMP TABLE base_eco_join AS
SELECT r.cokey, r.ecoclassid_std, r.ecoclassid, r.ecoclassname_std, q.plantsym, q.plantsciname, q.plantcomname, q.prodtype
  FROM main.coecoclass_codominant AS r
  LEFT JOIN base_unique_eco AS q ON r.ecoclassid_std = q.ecoclassid_std;

DROP TABLE IF EXISTS base_soil_join;
CREATE TEMP TABLE base_soil_join AS
SELECT f.mukey, f.area_ha, g.cokey, g.comppct_r, h.ecoclassid_std, h.ecoclassid, h.ecoclassname_std, h.plantsym, h.plantsciname, h.plantcomname, h.prodtype,
       (CAST(f.area_ha AS REAL) * g.comppct_r / 100) AS comp_ha, 
       COALESCE(i.prod, 0) AS prod
  FROM main.mupolygon AS f
  INNER JOIN main.component AS g ON f.mukey = g.mukey
  INNER JOIN base_eco_join AS h ON g.cokey = h.cokey
  LEFT JOIN base_prod_avg AS i ON h.cokey = i.cokey
        AND h.ecoclassid_std = i.ecoclassid_std
        AND h.plantsym = i.plantsym
        AND h.prodtype = i.prodtype;

DROP TABLE IF EXISTS prod_wgt;
CREATE TEMP TABLE prod_wgt AS
SELECT coalesce(q.ecogroup, w.ecoclassid_std) ecogroup, 
	   CASE WHEN q.ecoid IS NULL THEN 'ecosite' ELSE 'ecogroup' END grouptype,
	   w.plantsym, 
       MIN(w.plantsciname) AS plantsciname, MIN(w.plantcomname) AS plantcomname, w.prodtype, 
       SUM(w.comp_ha * w.prod) AS prodwgt, SUM(w.comp_ha) AS comp_ha
  FROM base_soil_join AS w
  LEFT JOIN main.ecogroup q ON w.ecoclassid_std = q.ecoid
 GROUP BY coalesce(q.ecogroup, w.ecoclassid_std), 
	      CASE WHEN q.ecoid IS NULL THEN 'ecosite' ELSE 'ecogroup' END,
	      w.plantsym, w.prodtype;

DROP TABLE IF EXISTS prod_wgt_sum;
CREATE TEMP TABLE prod_wgt_sum AS
SELECT v.ecogroup, v.grouptype, v.plantsym, v.plantsciname, v.plantcomname, v.prodtype, 
       CASE WHEN (v.prodwgt IS NULL OR v.comp_ha IS NULL OR v.comp_ha = 0) THEN 0 
            ELSE ROUND(CAST(v.prodwgt/v.comp_ha AS numeric), 1) END AS prod
  FROM prod_wgt AS v;

DROP VIEW IF EXISTS ecogroup_plantprod;
CREATE TABLE ecogroup_plantprod AS
SELECT * FROM prod_wgt_sum;

