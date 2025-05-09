CREATE VIEW IF NOT EXISTS {schema}.cotaxmoistcl_first AS 
WITH moist_rn AS (
SELECT taxmoistcl, cokey, cotaxmckey, 
       row_number() over(partition by cokey order by cotaxmckey) rn
  FROM {schema}.cotaxmoistcl
)

SELECT * FROM moist_rn WHERE rn = 1;
