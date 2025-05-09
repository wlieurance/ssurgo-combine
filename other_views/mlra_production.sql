DROP VIEW IF EXISTS {schema}.mlra_production;
CREATE VIEW {schema}.mlra_production AS
WITH mlra AS (
SELECT *,
       CASE WHEN substr(ecoclassid_std, 1, 1) IN ('0','1','2', '3', '4', '5', '6', '7', '8', '9') 
             AND length(ecoclassid_std) = 10 THEN substring(ecoclassid_std, 1, 4)
             ELSE 'None' END mlra
  FROM {schema}.coecoclass_unique

), calc AS (
SELECT b.mlra, a.plantsym, a.plantsciname, a.plantcomname, a.prodtype, 
       sum(a.prod * coalesce(b.eco_ha, 0))/sum(b.eco_ha) prod_wgt_pct
  FROM {schema}.coecoclass_plantprod a
  LEFT JOIN mlra b ON a.ecoclassid_std = b.ecoclassid_std
  WHERE a.plantsym IS NOT NULL
  GROUP BY b.mlra, a.plantsym, a.plantsciname, a.plantcomname, a.prodtype
  
), total_calc AS (
SELECT mlra, prodtype, sum(prod_wgt_pct) total_pct 
  FROM calc
 GROUP BY mlra, prodtype
)

SELECT a.mlra, a.plantsym, a.plantsciname, a.plantcomname, a.prodtype,
       a.prod_wgt_pct * 100 /b.total_pct prod_wgt_pct
  FROM calc a
 INNER JOIN total_calc b ON a.mlra = b.mlra AND a.prodtype = b.prodtype
 ORDER BY a.mlra, prod_wgt_pct DESC;
