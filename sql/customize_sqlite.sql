DROP VIEW IF EXISTS texture_first;
DROP TABLE IF EXISTS texture_first;

DROP TABLE IF EXISTS texgrp_min;
CREATE TEMP TABLE texgrp_min AS 
SELECT chkey, min(chtgkey) AS chtgkey 
  FROM main.chtexturegrp 
 WHERE rvindicator = 'Yes'
 GROUP BY chkey;

DROP TABLE IF EXISTS texture_min;
CREATE TEMP TABLE texture_min AS
SELECT chtgkey, min(chtkey) AS chtkey 
  FROM main.chtexture
 GROUP BY chtgkey;

DROP TABLE IF EXISTS texture_full;
CREATE TEMP TABLE texture_full AS
SELECT x.* 
  FROM main.chtexture AS x
 INNER JOIN texture_min AS y ON x.chtkey = y.chtkey;

DROP TABLE IF EXISTS texture_first;
CREATE TABLE texture_first AS
SELECT a.*, c.texcl, c.lieutex, c.chtkey 
  FROM main.chtexturegrp AS a
 INNER JOIN texgrp_min AS b ON a.chtgkey = b.chtgkey
 INNER JOIN texture_full AS c ON b.chtgkey = c.chtgkey;

CREATE UNIQUE INDEX IF NOT EXISTS texture_first_chkey ON texture_first (chkey);
CREATE UNIQUE INDEX IF NOT EXISTS texture_first_chtgkey ON texture_first (chtgkey);
CREATE UNIQUE INDEX IF NOT EXISTS texture_first_chtkey ON texture_first (chtkey);
