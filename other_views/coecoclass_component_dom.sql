DROP VIEW IF EXISTS {schema}.coecoclass_component_dom;
CREATE VIEW {schema}.coecoclass_component_dom AS
--determines which component is dominant by area for each ecological site
WITH area_calc AS (
SELECT a.mukey, a.cokey, c.ecoclassid_std, (a.comppct_r/100.0) * b.area_ha comp_ha
  FROM {schema}.component a
 INNER JOIN {schema}.mupolygon b ON a.mukey = b.mukey
 INNER JOIN {schema}.coecoclass_codominant c ON a.cokey = c.cokey
 WHERE c.ecoclassid_std IS NOT NULL

-- sums cokey areas for multipler possible map units
), area_sum AS (
SELECT ecoclassid_std, cokey, sum(comp_ha) comp_ha
  FROM area_calc
 GROUP BY ecoclassid_std, cokey

), comp_rank AS (
SELECT ecoclassid_std, cokey, comp_ha,
       row_number() over(partition by ecoclassid_std order by comp_ha DESC) rn
  FROM area_sum
)

SELECT ecoclassid_std, cokey, comp_ha 
  FROM comp_rank 
 WHERE rn = 1;

