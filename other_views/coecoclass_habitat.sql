DROP MATERIALIZED VIEW IF EXISTS soil.coecoclass_habitat;
CREATE MATERIALIZED VIEW soil.coecoclass_habitat AS
WITH prod_sum AS (
SELECT ecoclassid_std, ecoclassid, ecoclassname_std, plantsym, plantsciname, plantcomname, sum(prod) prod
  FROM soil.coecoclass_plantprod
 GROUP BY  ecoclassid_std, ecoclassid, ecoclassname_std, plantsym, plantsciname, plantcomname

), gh AS (
SELECT a.ecoclassid_std, a.ecoclassname_std, coalesce(b.accepted_symbol, a.plantsym) plantsym, 
       coalesce(b.scientific_name, a.plantsciname) plantsciname, 
       coalesce(b.common_name, a.plantcomname) plantcomname,
       b.code_type, b.duration_first duration, b.growth_habit_first growth_habit,
       CASE WHEN b.duration_first = 'Perennial' THEN 1
            WHEN b.duration_first = 'Biennial' THEN 2
            WHEN b.duration_first = 'Annual' THEN 3
            ELSE 4 END duration_order,
       CASE WHEN b.growth_habit_first = 'Tree' THEN 1
            WHEN b.growth_habit_first IN ('Shrub', 'Subshrub') THEN 2
            WHEN b.growth_habit_first = 'Graminoid' THEN 3
            WHEN b.growth_habit_first IN ('Forb/herb', 'Vine') THEN 4
            WHEN b.growth_habit_first IN ('Nonvascular', 'Lichenous') THEN 5
            ELSE 6 END gh_order,
       prod
  FROM prod_sum a
  LEFT JOIN plant.plant_mod b ON a.plantsym = b.symbol

), rn AS (
SELECT *, row_number() over(partition by ecoclassid_std, gh_order order by prod DESC) rn
  FROM gh
 WHERE duration = 'Perennial' 
   AND growth_habit IN ('Tree', 'Shrub', 'Subshrub', 'Graminoid')
   AND code_type != 'unknown'

), gh_group AS (
SELECT ecoclassid_std, ecoclassname_std, gh_order, string_agg(plantsym, '-') plants, 
       string_agg(round(prod, 0)::varchar, '-') prods
  FROM rn
 WHERE rn < 3
 GROUP BY ecoclassid_std, ecoclassname_std, gh_order
    
), eco_group AS (
SELECT ecoclassid_std, ecoclassname_std, string_agg(gh_order::varchar, ';') gh, 
       string_agg(plants, '/') habitat, string_agg(prods, '/') prod
  FROM gh_group
 GROUP BY ecoclassid_std, ecoclassname_std

), eco_final AS (
SELECT ecoclassid_std, ecoclassname_std, 
       CASE WHEN gh = '3' THEN '/' || habitat ELSE habitat END habitat,
       CASE WHEN gh = '3' THEN '/' || prod ELSE prod END prod
  FROM eco_group
)

SELECT * FROM eco_final;
