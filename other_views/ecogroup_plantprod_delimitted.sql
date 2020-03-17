--creates a comma delimited list of plants in an ecogroup in descending production order
SELECT a.ecogroup, coalesce(b.production, c.production) AS production
  FROM (SELECT ecogroup FROM soil.ecogroup_plantprod GROUP BY ecogroup) AS a
  LEFT JOIN (
		SELECT ecogroup, string_agg(plantsym, ',') AS production
		  FROM (
			SELECT ecogroup, plantsym, prodtype, prod
				FROM soil.ecogroup_plantprod
				WHERE prodtype = 'range'
				ORDER BY ecogroup, prod DESC, plantsym) AS a
		 GROUP BY ecogroup, prodtype) AS b ON a.ecogroup = b.ecogroup
  LEFT JOIN (
  		SELECT ecogroup, string_agg(plantsym, ',') AS production
		  FROM (
			SELECT ecogroup, plantsym, prodtype, prod
				FROM soil.ecogroup_plantprod
				WHERE prodtype LIKE 'forest%'
				ORDER BY ecogroup, prod DESC, plantsym) AS a
		 GROUP BY ecogroup, prodtype) AS c ON a.ecogroup = c.ecogroup
 ORDER BY ecogroup;