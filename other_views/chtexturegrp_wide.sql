WITH rns AS (
SELECT texdesc, chkey,
       row_number() over(partition by chkey order by rvindicator desc, chtgkey) rn
  FROM chtexturegrp

), tex_1 AS (
SELECT texdesc, chkey 
  FROM rns 
 WHERE rn = 1

), tex_2 AS (
SELECT texdesc, chkey 
  FROM rns 
 WHERE rn = 2

), tex_3 AS (
SELECT texdesc, chkey 
  FROM rns 
 WHERE rn = 3

), tex_join AS (
SELECT a.chkey, 
       a.texdesc texdesc_1,
	   b.texdesc texdesc_2,
	   c.texdesc texdesc_3
  FROM tex_1 a
  LEFT JOIN tex_2 b ON a.chkey = b.chkey
  LEFT JOIN tex_3 c ON a.chkey = c.chkey
)

SELECT * FROM tex_join;