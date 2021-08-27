SELECT ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, ecotype, sum(eco_ha) AS eco_ha
  FROM soil.coecoclass_area
 WHERE eco_ha IS NOT NULL
 GROUP BY ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, ecotype
 ORDER BY ecoclassid_std