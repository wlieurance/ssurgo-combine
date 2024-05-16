-- Creates a wide table givng the 2 most dominant species (sci. name) for each growth habit
-- (tree, shrub/shubsrub, graminoid, forb)
DROP MATERIALIZED VIEW IF EXISTS soil.coecoclass_habitat_crosstab;
CREATE MATERIALIZED VIEW soil.coecoclass_habitat_crosstab AS
SELECT * FROM crosstab(
$$
WITH prod_sum AS (
SELECT ecoclassid_std, ecoclassid, ecoclassname_std, plantsym, plantsciname, plantcomname, sum(prod) prod
  FROM soil.coecoclass_plantprod
 GROUP BY  ecoclassid_std, ecoclassid, ecoclassname_std, plantsym, plantsciname, plantcomname

), gh AS (
SELECT a.ecoclassid_std, a.ecoclassname_std, coalesce(b.accepted_symbol, a.plantsym) plantsym, 
       coalesce(b.scientific_name, a.plantsciname) plantsciname, 
       coalesce(b.common_name, a.plantcomname) plantcomname,
       b.code_type, b.duration_first duration, b.growth_habit_first growth_habit,
       CASE WHEN b.growth_habit_first = 'Tree' THEN 'tree'
            WHEN b.growth_habit_first IN ('Shrub', 'Subshrub') THEN 'shrub'
            WHEN b.growth_habit_first = 'Graminoid' THEN 'graminoid'
            WHEN b.growth_habit_first IN ('Forb/herb', 'Vine') THEN 'forb'
            WHEN b.growth_habit_first IN ('Nonvascular', 'Lichenous') THEN 'nonvascular'
            ELSE lower(growth_habit_first) END gh_class,
       prod
  FROM prod_sum a
  LEFT JOIN plant.plant_mod b ON a.plantsym = b.symbol

), rn AS (
SELECT *, row_number() over(partition by ecoclassid_std, gh_class order by prod DESC) rn
  FROM gh
 WHERE gh_class IN ('tree', 'shrub', 'graminoid', 'forb') 
   AND code_type != 'unknown'

), class_concat AS (
SELECT ecoclassid_std, gh_class, string_agg(plantsciname, ', ' ORDER BY rn) dom2
  FROM rn  
 WHERE rn < 3 
 GROUP BY ecoclassid_std, gh_class

)

SELECT * FROM class_concat ORDER BY 1,2
$$, 

$$VALUES ('tree'::text), ('shrub'::text), ('graminoid'::text), ('forb'::text)$$
) AS ct(ecoclassid_std TEXT, tree TEXT, shrub TEXT, graminoid TEXT, forb TEXT);
