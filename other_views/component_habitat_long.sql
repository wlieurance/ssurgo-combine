DROP VIEW IF EXISTS {schema}.component_habitat_long;
CREATE VIEW {schema}.component_habitat_long AS

WITH prod_sum AS (
SELECT cokey, plantsym, plantsciname, plantcomname, sum(prod) prod
  FROM {schema}.component_plantprod
 GROUP BY cokey, plantsym, plantsciname, plantcomname

), gh AS (
SELECT a.cokey, coalesce(b.accepted_symbol, a.plantsym) plantsym, 
       coalesce(b.scientific_name, a.plantsciname) plantsciname, 
       coalesce(b.common_name, a.plantcomname) plantcomname,
       b.code_type, b.duration_first duration, c.growth_habit_first growth_habit,
       CASE WHEN c.growth_habit_first IN ('Shrub', 'Subshrub') 
              OR b.scientific_name LIKE 'Artemisia tridentata%' THEN 2
            WHEN c.growth_habit_first = 'Tree' THEN 1
            WHEN c.growth_habit_first IN ('Graminoid', 'Forb/herb', 'Vine', 'Nonvascular', 'Lichenous') THEN 3
            ELSE 4 END gh_order,
       prod
  FROM prod_sum a
  LEFT JOIN plant.plant_mod b ON a.plantsym = b.symbol
  LEFT JOIN plant.growth_habits_probable c ON b.symbol = c.symbol

), rn AS (
SELECT *, row_number() over(partition by cokey, gh_order order by prod DESC) rn
  FROM gh
 WHERE code_type != 'unknown'
)

SELECT * FROM rn;
