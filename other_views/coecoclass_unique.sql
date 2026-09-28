CREATE VIEW soil.ecoclass AS

WITH eco AS (
SELECT string_agg(ecoclasstypename, ';') AS ecoclasstypename, ecoclassid
  FROM soil.coecoclass
 GROUP BY ecoclassid

), eco_regexp AS (
SELECT ecoclasstypename, ecoclassid, regexp_matches(ecoclassid, '^([RFG])?(\d{3})([A-Za-z])([A-Za-z])(\d{3})([A-Za-z]{2})_?(\d+)?$') AS eco_array
  FROM eco

), eco_split AS (
SELECT ecoclasstypename, ecoclassid, eco_array,
       CASE WHEN eco_array[1] = 'R'  OR lower(ecoclasstypename) LIKE '%range%' THEN 'range'
            WHEN eco_array[1] = 'F' OR lower(ecoclasstypename) LIKE '%forest%' THEN 'forest'
            WHEN eco_array[1] = 'G' THEN 'grassland'
            ELSE NULL END AS ecotype,
       eco_array[2] || eco_array[3] || eco_array[4] ||eco_array[5] || eco_array[6] AS ecoclassid_std,
       trim(leading '0' from eco_array[2]) || (CASE WHEN eco_array[3] = 'X' THEN '' ELSE eco_array[3] END) AS mlra,
       CASE WHEN eco_array[4] = 'Y' THEN NULL ELSE eco_array[4] END AS lru,
       eco_array[5] AS ecoclass_no, eco_array[6] AS state_fips, eco_array[7] AS sub_no
  FROM eco_regexp

), eco_join AS (
SELECT a.*, b.ecotype, b.ecoclassid_std, b.mlra, b.lru, b.ecoclass_no, b.state_fips, b.sub_no
  FROM eco AS a
  LEFT JOIN eco_split AS b ON a.ecoclassid = b.ecoclassid

)
  
SELECT * FROM eco_join