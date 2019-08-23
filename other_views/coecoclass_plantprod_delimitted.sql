--creates a comma delimitted list of plants in an ecosite in descending production order
SELECT a.ecoclassid_std, coalesce(b.production, c.production) AS production
  FROM (SELECT ecoclassid_std FROM soil.coecoclass_plantprod GROUP BY ecoclassid_std) AS a
  LEFT JOIN (
		SELECT ecoclassid_std, string_agg(plantsym, ',') AS production
		  FROM (
			SELECT ecoclassid_std, plantsym, prodtype, prod
				FROM soil.coecoclass_plantprod
				WHERE prodtype = 'range'
				ORDER BY ecoclassid_std, prod DESC, plantsym) AS a
		 GROUP BY ecoclassid_std, prodtype) AS b ON a.ecoclassid_std = b.ecoclassid_std
  LEFT JOIN (
  		SELECT ecoclassid_std, string_agg(plantsym, ',') AS production
		  FROM (
			SELECT ecoclassid_std, plantsym, prodtype, prod
				FROM soil.coecoclass_plantprod
				WHERE prodtype LIKE 'forest%'
				ORDER BY ecoclassid_std, prod DESC, plantsym) AS a
		 GROUP BY ecoclassid_std, prodtype) AS c ON a.ecoclassid_std = c.ecoclassid_std
 ORDER BY ecoclassid_std;