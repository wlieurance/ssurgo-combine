WITH ordered AS ( 
SELECT plantsym, plantsciname, plantcomname, forestunprod, rangeprod, cokey, coeplantskey,
	   plantsym || '-' || rangeprod AS symprodr,
	   plantsym || '-' || forestunprod AS symprodf
  FROM soil.coeplants
 ORDER BY cokey, rangeprod DESC, forestunprod DESC

), grouped AS (
SELECT cokey, string_agg(symprodr, '; ') AS rangeprod, string_agg(symprodf, '; ') AS forestunprod
  FROM ordered
 GROUP BY cokey
)

SELECT * FROM grouped;