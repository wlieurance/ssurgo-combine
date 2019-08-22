import os
import csv
import argparse
import sqlite3 as sqlite
import psycopg2
import extract
from config import spatialite_tables, postgis_tables


ecogroup_spatialite = [
"""/* This view creates a list of ecosites within ecogroups and attaches ecogroup metadata details */
CREATE VIEW IF NOT EXISTS {schema}.ecogroup_detail AS
SELECT a.ecoid, a.ecogroup, b.groupname, b.grouptype,
       CASE WHEN b.ecogroup IS NULL THEN NULL WHEN a.ecoid = b.modal_site THEN 1 ELSE 0 END AS modal,
       b.pub_status
  FROM {schema}.ecogroup AS a
  LEFT JOIN {schema}.ecogroup_meta AS b ON a.ecogroup = b.ecogroup;""",

"""/* This  view ranks ecogroup per map unit by area percent from values listed in the ecogroup table. 
Ranking removed due to ineffeciency with SQLite (No Row_Number() Function) */
CREATE VIEW IF NOT EXISTS {schema}.ecogroup_mapunit_ranked AS
SELECT f.mukey, f.ecogroup, f.groupname, f.grouptype, f.pub_status, f.ecogrouppct 
  FROM (
       SELECT b.mukey, b.ecogroup, b.groupname, b.grouptype, b.pub_status, b.ecogrouppct 
         FROM (
              SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype, a.pub_status, sum(a.ecoclasspct) AS ecogrouppct 
                FROM (
                     SELECT m.mukey, m.ecoclassid, m.ecoclassid_std, m.ecoclassname,
                            CASE WHEN n.ecogroup IS NULL THEN (CASE WHEN m.ecoclassname IS NULL THEN NULL ELSE 
                                      LOWER(LTRIM(RTRIM(REPLACE(REPLACE(m.ecoclassname,'"',' in'),'  ',' ')))) END)
                                 ELSE n.groupname END AS groupname, 
                            m.ecoclasspct,
                            CASE WHEN m.ecoclassid_std IS NULL THEN NULL
                                 WHEN n.ecogroup IS NULL THEN m.ecoclassid_std 
                                 ELSE n.ecogroup END AS ecogroup, 
                            CASE WHEN m.ecoclassid_std IS NULL THEN NULL
                                 WHEN n.ecogroup IS NULL THEN (CASE WHEN SUBSTR(m.ecoclassid, 1, 1) = 'R' THEN 'Range'
                                                                    WHEN SUBSTR(m.ecoclassid, 1, 1) = 'F' THEN 'Forest'
                                                                    ELSE NULL END)
                                 ELSE Null END AS ecosubgroup,
                            CASE WHEN m.ecoclassid_std IS NULL THEN 'Ecosite'
                                 WHEN n.ecogroup IS NULL THEN 'Ecosite'
                                 ELSE n.grouptype END AS grouptype, 
                            CASE WHEN m.ecoclassid_std IS NULL THEN NULL
                                 ELSE n.pub_status END AS pub_status
                       FROM (
                            SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, g.ecoclasspct
                              FROM (
                                   SELECT q.mukey, r.ecoclassid, Sum(q.comppct_r) AS ecoclasspct
                                     FROM {schema}.component AS q
                                     LEFT JOIN (
                                          SELECT y.*
                                            FROM {schema}.coecoclass AS y 
                                           INNER JOIN (
                                                 SELECT cokey, coecoclasskey
                                                   FROM (
                                                        SELECT a.cokey, a.coecoclasskey,  MAX(b.n) AS n 
                                                          FROM {schema}.coecoclass AS a 
                                                         INNER JOIN (
                                                               SELECT ecoclasstypename, Count(cokey) AS n
                                                                 FROM {schema}.coecoclass
                                                                GROUP BY ecoclasstypename
                                                               ) AS b ON a.ecoclasstypename = b.ecoclasstypename
                                                         GROUP BY a.cokey
                                                        ) AS x
                                                 ) AS z ON y.coecoclasskey = z.coecoclasskey
                                          ) AS r ON q.cokey = r.cokey
                                    GROUP BY mukey, ecoclassid
                                   ) AS g
                              LEFT JOIN (
                                   SELECT ecoclassid, 
                                          CASE WHEN SUBSTR(ecoclassid, 1, 1) IN ('F', 'R') THEN SUBSTR(ecoclassid, 2, 10) 
                                               WHEN SUBSTR(ecoclassid, 1, 1) = '0' THEN SUBSTR(ecoclassid, 1, 10) 
                                               ELSE ecoclassid END AS ecoclassid_std, 
                                          ecoclassname 
                                     FROM (
                                          SELECT ecoclassid, ecoclassname, MAX(n) AS n 
                                            FROM (
                                                 SELECT ecoclassid, ecoclassname, Count(ecoclassname) AS n
                                                   FROM {schema}.coecoclass
                                                  GROUP BY ecoclassid, ecoclassname
                                                  ORDER BY ecoclassid, Count(ecoclassname) DESC, ecoclassname
                                                 ) AS j
                                           GROUP BY ecoclassid
                                          ) AS k
                                   ) AS l ON g.ecoclassid = l.ecoclassid
                            ) AS m
                       LEFT JOIN {schema}.ecogroup_detail AS n on m.ecoclassid_std = n.ecoid
                     ) AS a
               GROUP BY mukey, ecogroup, groupname, grouptype, pub_status
              ) AS b
       ) AS f;""",

"""/* This  view ranks ecogroup per map unit by area percent values listed in the ecogroup table. */
CREATE VIEW IF NOT EXISTS {schema}.ecogroup_mudominant AS
SELECT mukey, ecogroup, groupname, grouptype, pub_status, MAX(ecogrouppct) AS ecogrouppct  
  FROM (SELECT * FROM {schema}.ecogroup_mapunit_ranked ORDER BY mukey ASC, ecogrouppct DESC, grouptype ASC, ecogroup ASC)
 GROUP BY mukey;""",

"""/*  Creates a list of unique ecogroups and calculates area statistics based on mupolygon.geom and component.comppct_r */
CREATE VIEW IF NOT EXISTS {schema}.ecogroup_unique AS
SELECT x.ecogroup, COUNT(x.mukey) AS group_n, 
        AVG(CAST(ecogrouppct AS float)) AS ecogrouppct_mean, SUM(group_ha) AS group_ha,
        groupname, grouptype, pub_status
    FROM (
        SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype, a.pub_status, 
               a.ecogrouppct, (a.ecogrouppct * b.mu_ha / 100) AS group_ha
            FROM {schema}.ecogroup_mapunit_ranked AS a
            LEFT JOIN (
                SELECT mukey, ST_Area(ST_Union(geom),1) * 0.0001 AS mu_ha
                    FROM {schema}.mupolygon
                GROUP BY mukey
                ) AS b ON a.mukey = b.mukey
        ) AS x
    GROUP BY x.ecogroup;""",

"""/* Isolates the six most dominant ecogroups/ecosites per mapunit and arranges them in a wide table format with their percentages
Ecogroup/ecosites percentages summed within map unit. join to mupolygon or mapunit via mukey. */
CREATE VIEW IF NOT EXISTS {schema}.ecogroup_wide AS
SELECT a.mukey, 
       n.ecogroup AS ecogroup_1, n.groupname AS groupname_1, n.ecogrouppct AS ecogrouppct_1, 
       o.ecogroup AS ecogroup_2, o.groupname AS groupname_2, o.ecogrouppct AS ecogrouppct_2, 
       p.ecogroup AS ecogroup_3, p.groupname AS groupname_3, p.ecogrouppct AS ecogrouppct_3, 
       q.ecogroup AS ecogroup_4, q.groupname AS groupname_4, q.ecogrouppct AS ecogrouppct_4, 
       r.ecogroup AS ecogroup_5, r.groupname AS groupname_5, r.ecogrouppct AS ecogrouppct_5, 
       s.ecogroup AS ecogroup_6, s.groupname AS groupname_6, s.ecogrouppct AS ecogrouppct_6
  FROM {schema}.mapunit AS a
  LEFT JOIN (
       SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct 
         FROM (
              SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct, 
                    (SELECT COUNT(*) 
                       FROM {schema}.ecogroup_mapunit_ranked AS b 
                      WHERE b.mukey = a.mukey AND 
                           (b.ecogrouppct > a.ecogrouppct OR
                            b.ecogrouppct = a.ecogrouppct AND 
                            b.ecogroup < a.ecogroup)) + 1 AS grouprank
                FROM {schema}.ecogroup_mapunit_ranked AS a
              )
        WHERE grouprank = 1
       ) AS n ON a.mukey = n.mukey
  LEFT JOIN (
       SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct 
         FROM (
              SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct, 
                    (SELECT COUNT(*) 
                       FROM {schema}.ecogroup_mapunit_ranked AS b 
                      WHERE b.mukey = a.mukey AND 
                           (b.ecogrouppct > a.ecogrouppct OR
                            b.ecogrouppct = a.ecogrouppct AND 
                            b.ecogroup < a.ecogroup)) + 1 AS grouprank
                FROM {schema}.ecogroup_mapunit_ranked AS a
              )
        WHERE grouprank = 2
       ) AS o ON a.mukey = o.mukey
  LEFT JOIN (
       SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct 
         FROM (
              SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct, 
                    (SELECT COUNT(*) 
                       FROM {schema}.ecogroup_mapunit_ranked AS b 
                      WHERE b.mukey = a.mukey AND 
                           (b.ecogrouppct > a.ecogrouppct OR
                            b.ecogrouppct = a.ecogrouppct AND 
                            b.ecogroup < a.ecogroup)) + 1 AS grouprank
                FROM {schema}.ecogroup_mapunit_ranked AS a
              )
        WHERE grouprank = 3
       ) AS p ON a.mukey = p.mukey
  LEFT JOIN (
       SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct 
         FROM (
              SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct, 
                    (SELECT COUNT(*) 
                       FROM {schema}.ecogroup_mapunit_ranked AS b 
                      WHERE b.mukey = a.mukey AND 
                           (b.ecogrouppct > a.ecogrouppct OR
                            b.ecogrouppct = a.ecogrouppct AND 
                            b.ecogroup < a.ecogroup)) + 1 AS grouprank
                FROM {schema}.ecogroup_mapunit_ranked AS a
              )
        WHERE grouprank = 4
       ) AS q ON a.mukey = q.mukey
  LEFT JOIN (
       SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct 
         FROM (
              SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct, 
                    (SELECT COUNT(*) 
                       FROM {schema}.ecogroup_mapunit_ranked AS b 
                      WHERE b.mukey = a.mukey AND 
                           (b.ecogrouppct > a.ecogrouppct OR
                            b.ecogrouppct = a.ecogrouppct AND 
                            b.ecogroup < a.ecogroup)) + 1 AS grouprank
                FROM {schema}.ecogroup_mapunit_ranked AS a
              )
        WHERE grouprank = 5
       ) AS r ON a.mukey = r.mukey
  LEFT JOIN (
       SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct 
         FROM (
              SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct, 
                    (SELECT COUNT(*) 
                       FROM {schema}.ecogroup_mapunit_ranked AS b 
                      WHERE b.mukey = a.mukey AND 
                           (b.ecogrouppct > a.ecogrouppct OR
                            b.ecogrouppct = a.ecogrouppct AND 
                            b.ecogroup < a.ecogroup)) + 1 AS grouprank
                FROM {schema}.ecogroup_mapunit_ranked AS a
              )
        WHERE grouprank = 6
       ) AS s ON a.mukey = s.mukey;""",
    
"""/* Calculates the hectares of each ecogroup within a map unit. */
 CREATE VIEW IF NOT EXISTS {schema}.ecogroup_area AS
 SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha,
        b.ecogroup, b.groupname, b.grouptype, b.pub_status, b.ecogrouppct,
        a.area_ha * b.ecogrouppct/100 AS ecogroup_ha
   FROM {schema}.mupolygon a
   LEFT JOIN {schema}.ecogroup_mapunit_ranked b ON a.mukey = b.mukey
  ORDER BY a.areasymbol, a.musym, b.ecogrouppct DESC;""",
    
"""/* Calculates area weighted plant production for each ecogroup. */
CREATE VIEW IF NOT EXISTS {schema}.ecogroup_plantprod AS
SELECT v.ecogroup, v.plantsym, v.plantsciname, v.plantcomname, v.prodtype, 
       CASE WHEN (v.prodwgt IS NULL OR v.comp_ha IS NULL OR v.comp_ha = 0) THEN 0 
            ELSE ROUND(CAST(v.prodwgt/v.comp_ha AS NUMERIC), 1) END AS prod
  FROM (
        SELECT w.ecogroup, w.plantsym, 
               MIN(w.plantsciname) AS plantsciname, MIN(w.plantcomname) AS plantcomname, w.prodtype, 
               SUM(w.comp_ha * w.prod) AS prodwgt, SUM(w.comp_ha) AS comp_ha
          FROM (
                SELECT f.mukey, f.area_ha, g.cokey, g.comppct_r, h.ecogroup, h.plantsym, h.plantsciname, h.plantcomname, h.prodtype,
                       (CAST(f.area_ha AS FLOAT) * g.comppct_r / 100) AS comp_ha, 
                       COALESCE(i.prod, 0) AS prod
                  FROM {schema}.mupolygon AS f
                  INNER JOIN {schema}.component AS g ON f.mukey = g.mukey
                  INNER JOIN (
                        SELECT i.cokey, i.ecogroup, 
                               q.plantsym, q.plantsciname, q.plantcomname, q.prodtype
                          FROM (
                               SELECT r.cokey, r.ecoclassid_std, r.ecoclassid, r.ecoclassname_std, COALESCE(s.ecogroup, r.ecoclassid_std) AS ecogroup
                                 FROM {schema}.coecoclass_codominant AS r
                                 LEFT JOIN {schema}.ecogroup AS s ON r.ecoclassid_std = s.ecoid) AS i
                          LEFT JOIN (
                                SELECT COALESCE(ecogroup, ecoclassid_std) AS ecogroup, plantsym, MIN(plantsciname) AS plantsciname, MIN(plantcomname) AS plantcomname, prodtype
                                  FROM (
                                        SELECT c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, q.ecogroup, 
                                               d.plantsym, d.plantsciname, d.plantcomname, d.rangeprod AS prod, 'range' AS prodtype 
                                          FROM {schema}.coecoclass_codominant AS c
                                         INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                          LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                         WHERE d.rangeprod IS NOT NULL
                                         UNION
                                        SELECT c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, q.ecogroup,
                                               d.plantsym, d.plantsciname, d.plantcomname, d.forestunprod AS prod, 'forest understory' AS prodtype
                                          FROM {schema}.coecoclass_codominant AS c
                                         INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                          LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                         WHERE d.forestunprod IS NOT NULL
                                       ) AS q
                                 GROUP BY COALESCE(ecogroup, ecoclassid_std), plantsym, prodtype
                                ) AS q ON i.ecogroup = q.ecogroup
                        ) AS h ON g.cokey = h.cokey
                  LEFT JOIN (
                        SELECT e.cokey,
                               COALESCE(e.ecogroup, e.ecoclassid_std) AS ecogroup, e.plantsym, e.prodtype, AVG(e.prod) AS prod
                          FROM (
                                SELECT c.cokey, c.ecoclassid_std, q.ecogroup, d.plantsym, d.rangeprod AS prod, 'range' AS prodtype
                                  FROM {schema}.coecoclass_codominant AS c
                                 INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                  LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                 WHERE d.rangeprod IS NOT NULL
                                 UNION
                                SELECT c.cokey, c.ecoclassid_std, q.ecogroup, d.plantsym, d.forestunprod AS prod, 'forest understory' AS prodtype
                                  FROM {schema}.coecoclass_codominant AS c
                                 INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                  LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                 WHERE d.forestunprod IS NOT NULL
                               ) AS e
                         GROUP BY e.cokey, COALESCE(e.ecogroup, e.ecoclassid_std), e.plantsym, e.prodtype
                       ) AS i ON h.cokey = i.cokey AND 
                                 h.ecogroup = i.ecogroup AND 
                                 h.plantsym = i.plantsym AND 
                                 h.prodtype = i.prodtype
               ) AS w
         GROUP BY w.ecogroup, w.plantsym, w.prodtype
      ) AS v;""",

"""SELECT DiscardGeometryColumn('ecogrouppolygon', 'geom');""",

"""DROP TABLE IF EXISTS {schema}.ecogrouppolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecogroup polygons. */
CREATE TABLE IF NOT EXISTS {schema}.ecogrouppolygon ( 
       ecogroup {limit_text} (50) PRIMARY KEY,
       groupname {limit_text} (100),
       grouptype {limit_text} (50),
       pub_status {limit_text} (20),
       area_ha {double},
       ecogrouppct {double});""",

"""SELECT AddGeometryColumn('ecogrouppolygon', 'geom', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecogroup per polygon with area percentage of ecogroup. Inserted into table for usefulness and speed. */
INSERT INTO {schema}.ecogrouppolygon (ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouppct, geom)
SELECT ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, geom
  FROM (
       SELECT ecogroup, groupname, grouptype, pub_status, ST_Area(geom, 1)/10000 AS area_ha, ecogrouparea_ha, ST_Multi(geom) AS geom
         FROM (
              SELECT ST_Union(geom) AS geom, ecogroup, groupname, grouptype,  
                     pub_status, sum(ecogrouparea_ha) AS ecogrouparea_ha
                FROM (
                     SELECT a.geom, COALESCE(b.ecogroup, 'NA') AS ecogroup, b.groupname, b.grouptype, b.pub_status,
                            a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha
                       FROM {schema}.mupolygon AS a
                       LEFT JOIN {schema}.ecogroup_mudominant AS b ON a.mukey = b.mukey)
                     AS x
               GROUP BY ecogroup) AS y)
       AS z;"""
]

ecogroup_postgis = [
"""/* This view creates a list of ecosites within ecogroups and attaches ecogroup metadata details */
CREATE OR REPLACE VIEW {schema}.ecogroup_detail AS
SELECT a.ecoid, a.ecogroup, b.groupname, b.grouptype,
       CASE WHEN b.ecogroup IS NULL THEN NULL WHEN a.ecoid = b.modal_site THEN 1 ELSE 0 END AS modal,
       b.pub_status
  FROM {schema}.ecogroup AS a
  LEFT JOIN {schema}.ecogroup_meta AS b ON a.ecogroup = b.ecogroup;""",

"""/* This  view ranks ecogroup per map unit by area percent from values listed in the ecogroup table. */
CREATE OR REPLACE VIEW {schema}.ecogroup_mapunit_ranked AS
SELECT f.mukey, f.ecogroup, f.groupname, f.grouptype, f.pub_status, f.ecogrouppct, f.grouprank 
  FROM (
       SELECT Row_Number() OVER (PARTITION BY b.mukey ORDER BY b.ecogrouppct DESC, b.grouptype ASC, b.ecogroup ASC) AS grouprank, 
              b.mukey, b.ecogroup, b.groupname, b.grouptype, b.pub_status, b.ecogrouppct 
         FROM (
              SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype, a.pub_status, sum(a.ecoclasspct) AS ecogrouppct 
                FROM (
                     SELECT m.mukey, m.ecoclassid, m.ecoclassid_std, m.ecoclassname, 
                            CASE WHEN n.ecogroup IS NULL THEN (CASE WHEN m.ecoclassname IS NULL THEN NULL ELSE 
                                      LOWER(LTRIM(RTRIM(REPLACE(REPLACE(m.ecoclassname,'"',' in'),'  ',' ')))) END)
                                 ELSE n.groupname END AS groupname, 
                            m.ecoclasspct,
                            CASE WHEN m.ecoclassid_std IS NULL THEN NULL
                                 WHEN n.ecogroup IS NULL THEN m.ecoclassid_std 
                                 ELSE n.ecogroup END AS ecogroup, 
                            CASE WHEN m.ecoclassid_std IS NULL THEN Null
                                 WHEN n.ecogroup IS NULL THEN (CASE WHEN LEFT(m.ecoclassid, 1) = 'R' THEN 'Range'
                                                                    WHEN LEFT(m.ecoclassid, 1) = 'F' THEN 'Forest'
                                                                    ELSE Null END)
                                 ELSE Null END AS ecosubgroup,
                            CASE WHEN m.ecoclassid_std IS NULL THEN 'Ecosite'
                                 WHEN n.ecogroup IS NULL THEN 'Ecosite'
                                 ELSE n.grouptype END AS grouptype, 
                            CASE WHEN m.ecoclassid_std IS NULL THEN Null
                                 ELSE n.pub_status END AS pub_status 
                       FROM (
                            SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, g.ecoclasspct, 
                                   ROW_NUMBER() OVER(PARTITION BY g.mukey ORDER BY g.ecoclasspct Desc, g.ecoclassid) AS RowNum
                              FROM (
                                   SELECT q.mukey, r.ecoclassid, Sum(q.comppct_r) AS ecoclasspct
                                     FROM {schema}.component AS q
                                     LEFT JOIN (
                                          SELECT y.*
                                            FROM {schema}.coecoclass AS y 
                                           INNER JOIN (
                                                 SELECT cokey, coecoclasskey
                                                   FROM (
                                                        SELECT a.cokey, a.coecoclasskey, b.n, 
                                                               ROW_NUMBER() OVER (PARTITION BY a.cokey ORDER BY b.n DESC) AS RowNum
                                                          FROM {schema}.coecoclass AS a 
                                                         INNER JOIN (
                                                               SELECT ecoclasstypename, Count(cokey) AS n
                                                                 FROM {schema}.coecoclass
                                                                GROUP BY ecoclasstypename
                                                               ) AS b ON a.ecoclasstypename = b.ecoclasstypename
                                                        ) AS x
                                                  WHERE RowNum = 1
                                                 ) AS z ON y.coecoclasskey = z.coecoclasskey
                                          ) AS r ON q.cokey = r.cokey
                                    GROUP BY mukey, ecoclassid
                                   ) AS g
                              LEFT JOIN (
                                   SELECT ecoclassid, 
                                          CASE WHEN SUBSTRING(ecoclassid, 1, 1) IN ('F', 'R') THEN SUBSTRING(ecoclassid, 2, 10) 
                                               WHEN SUBSTRING(ecoclassid, 1, 1) = '0' THEN SUBSTRING(ecoclassid, 1, 10) 
                                               ELSE ecoclassid END AS ecoclassid_std, 
                                          ecoclassname 
                                     FROM (
                                          SELECT ecoclassid, ecoclassname, n, 
                                                 ROW_NUMBER() OVER(PARTITION BY ecoclassid ORDER BY n DESC, ecoclassname) AS RowNum
                                            FROM (
                                                 SELECT ecoclassid, ecoclassname, Count(ecoclassname) AS n
                                                   FROM {schema}.coecoclass
                                                  GROUP BY ecoclassid, ecoclassname
                                                 ) AS j
                                          ) AS k
                                    WHERE RowNum = 1
                                   ) AS l ON g.ecoclassid = l.ecoclassid
                            ) AS m
                       LEFT JOIN {schema}.ecogroup_detail AS n on m.ecoclassid_std = n.ecoid
                     ) AS a
               GROUP BY mukey, ecogroup, groupname, grouptype, pub_status
              ) AS b
       ) AS f;""",
       
"""/* This  view ranks ecogroup per map unit by area percent values listed in the ecogroup table. */
CREATE OR REPLACE VIEW {schema}.ecogroup_mudominant AS
SELECT mukey, ecogroup, groupname, grouptype, pub_status, ecogrouppct  
  FROM {schema}.ecogroup_mapunit_ranked
 WHERE grouprank = 1;""",
 
""" /*  Creates a list of unique ecogroups and calculates area statistics based on mupolygon.geom and component.comppct_r */
CREATE OR REPLACE VIEW {schema}.ecogroup_unique AS
SELECT x.ecogroup, COUNT(x.mukey) AS group_n, 
        AVG(CAST(ecogrouppct AS float)) AS ecogrouppct_mean, SUM(group_ha) AS group_ha,
        MIN(groupname) AS groupname, MIN(grouptype) AS grouptype, MIN(pub_status) AS pubstatus
    FROM (
        SELECT a.mukey, a.ecogroup, a.groupname, a.grouptype, a.pub_status, 
               a.ecogrouppct, a.grouprank, (a.ecogrouppct * b.area_ha / 100) AS group_ha
            FROM {schema}.ecogroup_mapunit_ranked AS a
            LEFT JOIN (
                SELECT mukey, sum(area_ha) AS area_ha
                    FROM {schema}.mupolygon
                GROUP BY mukey
                ) AS b ON a.mukey = b.mukey
        ) AS x
    GROUP BY x.ecogroup;""",
 
"""/* Isolates the six most dominant ecogroups/ecosites per mapunit and arranges them in a wide table format with their percentages
Ecogroup/ecosites percentages summed within map unit.  join to mupolygon or mapunit via mukey. */
CREATE OR REPLACE VIEW {schema}.ecogroup_wide AS
SELECT x.mukey, 
       a.ecogroup AS ecogroup_1, a.groupname AS groupname_1, a.ecogrouppct AS ecogrouppct_1, 
       b.ecogroup AS ecogroup_2, b.groupname AS groupname_2, b.ecogrouppct AS ecogrouppct_2, 
       c.ecogroup AS ecogroup_3, c.groupname AS groupname_3, c.ecogrouppct AS ecogrouppct_3, 
       d.ecogroup AS ecogroup_4, d.groupname AS groupname_4, d.ecogrouppct AS ecogrouppct_4, 
       e.ecogroup AS ecogroup_5, e.groupname AS groupname_5, e.ecogrouppct AS ecogrouppct_5, 
       f.ecogroup AS ecogroup_6, f.groupname AS groupname_6, f.ecogrouppct AS ecogrouppct_6
  FROM {schema}.mapunit AS x
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=1) AS a ON x.mukey = a.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=2) AS b ON x.mukey = b.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=3) AS c ON x.mukey = c.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=4) AS d ON x.mukey = d.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=5) AS e ON x.mukey = e.mukey
  LEFT JOIN (SELECT * FROM {schema}.ecogroup_mapunit_ranked WHERE grouprank=6) AS f ON x.mukey = f.mukey;""",

"""/* Calculates the hectares of each ecogroup within a map unit. */
CREATE OR REPLACE VIEW {schema}.ecogroup_area AS
SELECT a.areasymbol, a.spatialver, a.musym, a.mukey, a.area_ha,
       b.ecogroup, b.groupname, b.grouptype, b.pub_status, b.ecogrouppct, b.grouprank,
       a.area_ha * b.ecogrouppct/100 AS ecogroup_ha
  FROM {schema}.mupolygon a
  LEFT JOIN {schema}.ecogroup_mapunit_ranked b ON a.mukey = b.mukey
 ORDER BY a.areasymbol, a.musym, b.grouprank;""",
  
"""/* Calculates area weighted plant production for each ecogroup. */
CREATE OR REPLACE VIEW {schema}.ecogroup_plantprod AS
SELECT v.ecogroup, v.plantsym, v.plantsciname, v.plantcomname, v.prodtype, 
       CASE WHEN (v.prodwgt IS NULL OR v.comp_ha IS NULL OR v.comp_ha = 0) THEN 0 
            ELSE ROUND(CAST(v.prodwgt/v.comp_ha AS NUMERIC), 1) END AS prod
  FROM (
        SELECT w.ecogroup, w.plantsym, 
               MIN(w.plantsciname) AS plantsciname, MIN(w.plantcomname) AS plantcomname, w.prodtype, 
               SUM(w.comp_ha * w.prod) AS prodwgt, SUM(w.comp_ha) AS comp_ha
          FROM (
                SELECT f.mukey, f.area_ha, g.cokey, g.comppct_r, h.ecogroup, h.plantsym, h.plantsciname, h.plantcomname, h.prodtype,
                       (CAST(f.area_ha AS FLOAT) * g.comppct_r / 100) AS comp_ha, 
                       COALESCE(i.prod, 0) AS prod
                  FROM {schema}.mupolygon AS f
                  INNER JOIN {schema}.component AS g ON f.mukey = g.mukey
                  INNER JOIN (
                        SELECT i.cokey, i.ecogroup, 
                               q.plantsym, q.plantsciname, q.plantcomname, q.prodtype
                          FROM (
                               SELECT r.cokey, r.ecoclassid_std, r.ecoclassid, r.ecoclassname_std, COALESCE(s.ecogroup, r.ecoclassid_std) AS ecogroup
                                 FROM {schema}.coecoclass_codominant AS r
                                 LEFT JOIN {schema}.ecogroup AS s ON r.ecoclassid_std = s.ecoid) AS i
                          LEFT JOIN (
                                SELECT COALESCE(ecogroup, ecoclassid_std) AS ecogroup, plantsym, MIN(plantsciname) AS plantsciname, MIN(plantcomname) AS plantcomname, prodtype
                                  FROM (
                                        SELECT c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, q.ecogroup, 
                                               d.plantsym, d.plantsciname, d.plantcomname, d.rangeprod AS prod, 'range' AS prodtype 
                                          FROM {schema}.coecoclass_codominant AS c
                                         INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                          LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                         WHERE d.rangeprod IS NOT NULL
                                         UNION
                                        SELECT c.ecoclassid, c.ecoclassid_std, c.ecoclassname_std, q.ecogroup,
                                               d.plantsym, d.plantsciname, d.plantcomname, d.forestunprod AS prod, 'forest understory' AS prodtype
                                          FROM {schema}.coecoclass_codominant AS c
                                         INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                          LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                         WHERE d.forestunprod IS NOT NULL
                                       ) AS q
                                 GROUP BY COALESCE(ecogroup, ecoclassid_std), plantsym, prodtype
                                ) AS q ON i.ecogroup = q.ecogroup
                        ) AS h ON g.cokey = h.cokey
                  LEFT JOIN (
                        SELECT e.cokey,
                               COALESCE(e.ecogroup, e.ecoclassid_std) AS ecogroup, e.plantsym, e.prodtype, AVG(e.prod) AS prod
                          FROM (
                                SELECT c.cokey, c.ecoclassid_std, q.ecogroup, d.plantsym, d.rangeprod AS prod, 'range' AS prodtype
                                  FROM {schema}.coecoclass_codominant AS c
                                 INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                  LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                 WHERE d.rangeprod IS NOT NULL
                                 UNION
                                SELECT c.cokey, c.ecoclassid_std, q.ecogroup, d.plantsym, d.forestunprod AS prod, 'forest understory' AS prodtype
                                  FROM {schema}.coecoclass_codominant AS c
                                 INNER JOIN {schema}.coeplants AS d ON c.cokey = d.cokey
                                  LEFT JOIN {schema}.ecogroup AS q ON c.ecoclassid_std = q.ecoid
                                 WHERE d.forestunprod IS NOT NULL
                               ) AS e
                         GROUP BY e.cokey, COALESCE(e.ecogroup, e.ecoclassid_std), e.plantsym, e.prodtype
                       ) AS i ON h.cokey = i.cokey AND 
                                 h.ecogroup = i.ecogroup AND 
                                 h.plantsym = i.plantsym AND 
                                 h.prodtype = i.prodtype
               ) AS w
         GROUP BY w.ecogroup, w.plantsym, w.prodtype
      ) AS v;""",

"""DROP TABLE IF EXISTS {schema}.ecogrouppolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecogroup polygons. */
CREATE TABLE IF NOT EXISTS {schema}.ecogrouppolygon (
       ecogroup {limit_text} (50) PRIMARY KEY,
       groupname {limit_text} (100),
       grouptype {limit_text} (50),
       pub_status {limit_text} (20),
       area_ha {double},
       ecogrouppct {double},
       geom geometry('MULTIPOLYGON', 4326));""",

#"""SELECT AddGeometryColumn('{schema}', 'ecogrouppolygon', 'geom', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecogroup per polygon with area percentage of ecogroup. Inserted into table for usefulness and speed. */
INSERT INTO {schema}.ecogrouppolygon (ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouppct, geom)
SELECT ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouparea_ha/area_ha AS ecogrouppct, geom
  FROM (
       SELECT ecogroup, groupname, grouptype, pub_status, ST_Area(geography(geom))/10000 AS area_ha, ecogrouparea_ha,
              ST_Multi(geom) AS geom
         FROM (
              SELECT ST_Union(geom) AS geom, ecogroup, Min(groupname) AS groupname,  Min(grouptype) AS grouptype, 
                     min(pub_status) AS pub_status, sum(ecogrouparea_ha) AS ecogrouparea_ha
               FROM (
                    SELECT a.geom, COALESCE(b.ecogroup, 'NA') AS ecogroup, b.groupname, b.grouptype, b.pub_status,
                           a.area_ha*(CAST(b.ecogrouppct AS REAL)/100) AS ecogrouparea_ha
                      FROM {schema}.mupolygon AS a
                      LEFT JOIN {schema}.ecogroup_mudominant AS b ON a.mukey = b.mukey
                    ) AS x
              GROUP BY ecogroup
              ) AS y)
       AS z;"""
]


def create_table(dbpath, schema, dbtype):
    sql_meta = ("CREATE TABLE IF NOT EXISTS {schema}.ecogroup_meta (ecogroup {limit_text} (20) PRIMARY KEY, "
                "groupname {limit_text} (100), grouptype {limit_text} (50), modal_site {limit_text} (20), "
                "pub_status {limit_text} (20));")
    sql = ("CREATE TABLE IF NOT EXISTS {schema}.ecogroup (ecoid {limit_text} (20) "
           "PRIMARY KEY, ecogroup {limit_text} (50) NOT NULL, FOREIGN KEY(ecogroup) "
           "REFERENCES {schema}.ecogroup_meta(ecogroup));")
    if dbtype == 'spatialite':
        spatialite_tables['schema'] = schema
        conn = sqlite.connect(dbpath)
        sql = sql.replace(' REFERENCES {schema}.', 'REFERENCES ')  # sqlite does not like schema in REFERENCES statement
        sql_meta = sql_meta.format(**spatialite_tables)
        sql = sql.format(**spatialite_tables)
    elif dbtype == 'postgis':
        postgis_tables['schema'] = schema
        conn = psycopg2.connect(dbpath)
        sql_meta = sql_meta.format(**postgis_tables)
        sql = sql.format(**postgis_tables)
    c = conn.cursor()
    c.execute(sql_meta)
    c.execute(sql)
    conn.commit()
    conn.close()


def load_ecogroups(dbpath, schema, dbtype, csvmetapath, csvpath):
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        paramstr = '?'
        ins1 = 'OR IGNORE'
        ins2 = ''
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        paramstr = '%s'
        ins1 = ''
        ins2 = 'ON CONFLICT DO NOTHING'
    c = conn.cursor()
    if csvmetapath:
        c.execute("DELETE FROM {schema}.ecogroup;".format({'schema': schema}))
        c.execute("DELETE FROM {schema}.ecogroup_meta;".format({'schema': schema}))
        with open(csvmetapath, 'r') as csvfile_meta:
            csvreader_meta = csv.reader(csvfile_meta, delimiter=',')
            headers = next(csvreader_meta)
            SQL = "INSERT {{ins1}} INTO {{schema}}.ecogroup_meta (ecogroup, groupname, grouptype, modal_site, " \
                  "pub_status) VALUES ({!s}) {{ins2}};".format(','.join([paramstr]*5))
            SQL = SQL.format(**{'ins1': ins1, 'schema': schema, 'ins2': ins2})
            for row in csvreader_meta:
                if row:
                    # print(row)
                    convert = []
                    for r in row:
                        if r:
                            convert.append(r)
                        else:
                            convert.append(None)
                    c.execute(SQL, (convert[0], convert[1], convert[2], convert[3], convert[4]))
        conn.commit()
    else:
        print("no ecogroup metadata file provided. Skipping data load...")
    if csvmetapath:
        with open(csvpath, 'r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            headers = next(csvreader)
            SQL = "INSERT {{ins1}} INTO {{schema}}.ecogroup (ecoid, ecogroup) " \
                  "VALUES ({!s}) {{ins2}};".format(','.join([paramstr]*2))
            SQL = SQL.format(**{'ins1': ins1, 'schema': schema, 'ins2': ins2})
            for row in csvreader:
                convert = []
                for r in row:
                    if r:
                        convert.append(r)
                    else:
                        convert.append(None)
                c.execute(SQL, (convert[0], convert[1]))
        conn.commit()
    else:
        print("no ecogroup file provided. Skipping data load...")
    conn.close()


def create_views(dbpath, schema, dbtype):
    print("Creating ecogroup objects...")
    if dbtype == 'spatialite':
        spatialite_tables['schema'] = schema
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('mod_spatialite');")
        c = conn.cursor()
        for stmt in ecogroup_spatialite:
            c.execute(stmt.format(**spatialite_tables))
    elif dbtype == 'postgis':
        postgis_tables['schema'] = schema
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        for stmt in ecogroup_postgis:
            c.execute(stmt.format(**postgis_tables))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description=
                                     'This script will add ecological groupings to an already created SSURGO database. '
                                     'The ecogroups table will be created and populated via a comma delimited file, '
                                     'and the ecogrouppolygon spatial feature will be created.')
    # positional arguments
    parser.add_argument('dbpath', help='path of the sqlite SSURGO created via main.py within this toolset')
    parser.add_argument('csvpaths', help='paths of the csv files containing the ecological group metadata and the sites '
                                         'within each ecogroup ("path/to/ecogroup_meta.csv","/path/to/ecogroup.csv") '
                                         '(see ecogroups_example.csv and ecogroups_meta_example.csv) (tab delimited)')
    parser.add_argument('-t', '--type', metavar='DATABASE_TYPE', default='spatialite',
                        help='decides which database type to import to, spatialite or postgis')
    parser.add_argument('-x', '--schema', default='',
                        help='tells the script what schema to use')
    args = parser.parse_args()

    # check for valid arguments
    if args.type == 'spatialite':
        if not os.path.isfile(args.dbpath):
            print("dbpath does not exist. Please choose an existing SSURGO file to use.")
            quit()
    csvmetapath = args.csvpaths.split(',')[0].strip().replace('"', '')
    csvpath = args.csvpaths.split(',')[1].strip().replace('"', '')
    if not os.path.isfile(csvmetapath):
        print(csvmetapath + " does not exist. Please choose an existing comma delimited ecogroup_meta file to use.")
        quit()
    if not os.path.isfile(csvpath):
        print(csvpath + " does not exist. Please choose an existing comma delimited ecogroup file to use.")
        quit()
    
    schema = extract.get_default_schema(args.type, args.schema)
    create_table(args.dbpath, schema, args.type)
    load_ecogroups(args.dbpath, schema, args.type, csvmetapath, csvpath)
    create_views(args.dbpath, schema, args.type)
    print('Script finished.')
