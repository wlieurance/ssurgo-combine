CREATE VIEW soil.coecoclass_component_dom AS
WITH area_calc AS (
SELECT a.mukey, a.cokey, a.ecoclassid_std, (a.comppct_r/100.0) * b.area_ha comp_ha
  FROM soil.component_mapunit_detail a
 INNER JOIN soil.mupolygon b ON a.mukey = b.mukey
 WHERE a.ecoclassid_std IS NOT NULL

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
