spatialite_views = [
"""/* This query returns the surface soil horizon and should be joined 
to the component or qry_component_dominant via the cokey in arcGIS */
CREATE VIEW IF NOT EXISTS chorizon_surface AS
SELECT * 
FROM CHORIZON 
WHERE hzdept_r = 0;""",

"""/* This selects the top layer from chorizon that is not organic material */
CREATE VIEW IF NOT EXISTS chorizon_surface_noOM AS
SELECT *, min(hzdept_r)
  FROM chorizon 
 WHERE desgnmaster != 'O'
 GROUP BY cokey;""",

"""/* View isolates only one record in the case of multiple records per soil horizon
join to chorizon or qry_chorizon_surface via chkey */
CREATE VIEW IF NOT EXISTS chunified_first AS
SELECT unifiedcl, max(rvindicator) AS rvindicator, chkey, chunifiedkey
  FROM chunified
 GROUP BY chkey;""",

"""/* Isolates one ecosite per component by ecoclasstype in the case of multiple ecosites per component 
(in order of ecoclasstype abundance within the database). Join to component or qry_component_dominant via cokey. */
CREATE VIEW IF NOT EXISTS coecoclass_codominant AS
SELECT y.*, 
       CASE WHEN substr(ecoclassid, 1,1) IN ('F','R') THEN substr(ecoclassid, 2, 10) 
            WHEN substr(ecoclassid, 1,1) = '0' THEN substr(ecoclassid, 1, 10) 
            ELSE ecoclassid END AS ecoclassid_std, 
       lower(trim(replace(replace(ecoclassname,'"',' in'),'  ',' '))) AS ecoclassname_std 
  FROM coecoclass AS y
 INNER JOIN (SELECT a.cokey, a.coecoclasskey, max(b.n) AS n 
               FROM COECOCLASS AS a 
              INNER JOIN (SELECT ecoclasstypename, Count(cokey) AS n 
		                    FROM coecoclass 
		                   GROUP BY ecoclasstypename ) AS b ON a.ecoclasstypename = b.ecoclasstypename
              GROUP BY a.cokey, a.coecoclasskey) AS x ON y.coecoclasskey = x.coecoclasskey;""",

"""/* Identifies the dominant ecosite per map unit by summing up comppct_r values*/
CREATE VIEW IF NOT EXISTS coecoclass_mudominant AS
SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, l.ecoclassname_std, l.ecotype, max(g.ecoclasspct) AS ecoclasspct
  FROM (
       SELECT q.mukey, ifnull(r.ecoclassid, 'NA') AS ecoclassid, Sum(q.comppct_r) AS ecoclasspct
         FROM component AS q
         LEFT JOIN (
              SELECT y.*
                FROM coecoclass AS y 
               INNER JOIN (
                     SELECT a.cokey, a.coecoclasskey, max(b.n) AS n
                       FROM coecoclass AS a 
                      INNER JOIN (
                            SELECT ecoclasstypename, count(cokey) AS n
                              FROM coecoclass
                             GROUP BY ecoclasstypename)
                            AS b ON a.ecoclasstypename = b.ecoclasstypename
                      GROUP BY cokey)
                      AS x ON y.coecoclasskey = x.coecoclasskey)
              AS r ON q.cokey = r.cokey
        GROUP BY mukey, ecoclassid)
       AS g
  LEFT JOIN (
       SELECT ecoclassid, 
              CASE WHEN substr(ecoclassid, 1, 1) IN ('F', 'R') THEN substr(ecoclassid, 2, 10) 
                   WHEN substr(ecoclassid, 1, 1) = '0' THEN substr(ecoclassid, 1, 10) 
                   ELSE ecoclassid END AS ecoclassid_std,
	      CASE WHEN substr(ecoclassid, 1, 1) = 'F' THEN 'forest'
	           WHEN substr(ecoclassid, 1, 1) = 'R' THEN 'range'
                   ELSE NULL END AS ecotype,
              ecoclassname,
              lower(trim(replace(replace(ecoclassname,'"',' in'),'  ',' '))) AS ecoclassname_std
         FROM (
              SELECT ecoclassid, ecoclassname, max(n) AS n
                FROM (
                     SELECT ecoclassid, ecoclassname, Count(ecoclassname) AS n
                       FROM COECOCLASS
                      GROUP BY ecoclassid, ecoclassname)
                     AS t
              GROUP BY ecoclassid) 
              AS j)
       AS l ON g.ecoclassid = l.ecoclassid
 GROUP BY mukey;""",

"""/* Gives dominant and maximum flooding/ponding frequency class and flooding/ponding frequency duration grouped by comonth.
Join to comonent or qry_component_dominant via cokey. */
CREATE VIEW IF NOT EXISTS comonth_flodpondfreqcl AS
SELECT a.cokey, 
       b.flodfreqcl AS flodfreqcl_max, d.flodfreqcl AS flodfreqcl_dom, d.flodfreqcl_n AS flodfreqcl_dom_n,
       c.floddurcl AS floddurcl_max, e.floddurcl AS floddurcl_dom, e.floddurcl_n AS floddurcl_dom_n,
       f.pondfreqcl AS pondfreqcl_max, h.pondfreqcl AS pondfreqcl_dom, h.pondfreqcl_n AS pondfreqcl_dom_n,
       g.ponddurcl AS ponddurcl_max, i.ponddurcl AS ponddurcl_dom, i.ponddurcl_n AS ponddurcl_dom_n
  FROM (SELECT cokey FROM comonth GROUP BY cokey) AS a
  --flooding section
  LEFT JOIN (
       SELECT cokey, flodfreqcl, 
              max(CASE WHEN flodfreqcl IS NULL THEN NULL
                       WHEN flodfreqcl = 'None' THEN 1 
                       WHEN flodfreqcl = 'Rare' THEN 2 
                       WHEN flodfreqcl = 'Occasional' THEN 3 
                       WHEN flodfreqcl = 'Frequent' THEN 4 
                       ELSE NULL END) AS flodfreqcl_max
         FROM comonth
        GROUP BY cokey)
        AS b ON a.cokey = b.cokey
  LEFT JOIN (
       SELECT cokey, floddurcl,            
              max(CASE WHEN floddurcl IS NULL THEN NULL
                       WHEN floddurcl = 'Extremely brief (0.1 to 4 hours)' THEN 1 
                       WHEN floddurcl = 'Very brief (4 to 48 hours)' THEN 2 
                       WHEN floddurcl = 'Brief (2 to 7 days)' THEN 3 
                       WHEN floddurcl = 'Long (7 to 30 days)' THEN 4 
                       WHEN floddurcl = 'Very long (more than 30 days)' THEN 5 
                       ELSE NULL END) AS floddurcl_max
         FROM comonth
        GROUP BY cokey)
        AS c ON a.cokey = c.cokey
  LEFT JOIN (
       SELECT x.cokey, x.flodfreqcl, max(x.flodfreqcl_n) AS flodfreqcl_n
         FROM (
              SELECT cokey, flodfreqcl, count(flodfreqcl) AS flodfreqcl_n
                FROM comonth
               GROUP BY cokey, flodfreqcl) 
               AS x
        GROUP BY cokey)
       AS d ON a.cokey = d.cokey
  LEFT JOIN (
       SELECT y.cokey, y.floddurcl, max(y.floddurcl_n) AS floddurcl_n
         FROM (
              SELECT cokey, floddurcl, count(floddurcl) AS floddurcl_n    
                FROM comonth
               GROUP BY cokey, floddurcl)
              AS y
        GROUP BY cokey)     
       AS e ON a.cokey = e.cokey
  --ponding section 
  LEFT JOIN (
       SELECT cokey, pondfreqcl, 
              max(CASE WHEN pondfreqcl IS NULL THEN NULL
                       WHEN pondfreqcl = 'None' THEN 1 
                       WHEN pondfreqcl = 'Rare' THEN 2 
                       WHEN pondfreqcl = 'Occasional' THEN 3 
                       WHEN pondfreqcl = 'Frequent' THEN 4 
                       ELSE NULL END) AS pondfreqcl_max
         FROM comonth
        GROUP BY cokey)
        AS f ON a.cokey = f.cokey
  LEFT JOIN (
       SELECT cokey, ponddurcl,            
              max(CASE WHEN ponddurcl IS NULL THEN NULL
                       WHEN ponddurcl = 'Extremely brief (0.1 to 4 hours)' THEN 1 
                       WHEN ponddurcl = 'Very brief (4 to 48 hours)' THEN 2 
                       WHEN ponddurcl = 'Brief (2 to 7 days)' THEN 3 
                       WHEN ponddurcl = 'Long (7 to 30 days)' THEN 4 
                       WHEN ponddurcl = 'Very long (more than 30 days)' THEN 5 
                       ELSE NULL END) AS ponddurcl_max
         FROM comonth
        GROUP BY cokey)
        AS g ON a.cokey = g.cokey
  LEFT JOIN (
       SELECT x.cokey, x.pondfreqcl, max(x.pondfreqcl_n) AS pondfreqcl_n
         FROM (
              SELECT cokey, pondfreqcl, count(pondfreqcl) AS pondfreqcl_n
                FROM comonth
               GROUP BY cokey, pondfreqcl) 
               AS x
        GROUP BY cokey)
       AS h ON a.cokey = h.cokey
  LEFT JOIN (
       SELECT y.cokey, y.ponddurcl, max(y.ponddurcl_n) AS ponddurcl_n
         FROM (
              SELECT cokey, ponddurcl, count(ponddurcl) AS ponddurcl_n    
                FROM comonth
               GROUP BY cokey, ponddurcl)
              AS y
        GROUP BY cokey)     
       AS i ON a.cokey = i.cokey;""",

"""/* Provides minimum and maximum soil moisture depth levels group by comonth.  Join to component or qry_component_dominant */
CREATE VIEW IF NOT EXISTS comonth_soimoistdept AS
SELECT a.cokey, min(b.soimoistdept_r) AS soimoistdept_rmin, max(b.soimoistdept_r) AS soimoistdept_rmax 
FROM comonth AS a 
INNER JOIN cosoilmoist AS b ON a.comonthkey = b.comonthkey 
WHERE soimoiststat = 'Wet' 
GROUP BY a.cokey;""",

"""/* Provides the most dominant component per map unit by comppct_r (highest) and compname (descending alphabetical order) in case of tie.
Join to mupolygon or mapunit via mukey. */
CREATE VIEW IF NOT EXISTS component_dominant AS
SELECT a.* 
  FROM component AS a
 INNER JOIN (
       SELECT mukey, max(comppct_r) AS comppct_r, cokey
         FROM (
              SELECT mukey, comppct_r, compname, cokey
                FROM component
               ORDER BY mukey, comppct_r DESC, compname ASC)
              AS x
        GROUP BY mukey)
       AS b ON a.cokey = b.cokey ;""",

"""/* In the case that there is more than one texture available per horizon, returns the one with the alphabetically lowest chkey.
Join to chorizon or qry_chorizon_surface via chkey. */
CREATE VIEW IF NOT EXISTS texture_first AS
SELECT a.*, c.texcl, c.lieutex, c.chtkey FROM chtexturegrp AS a
INNER JOIN (
      SELECT chkey, Min(chtgkey) AS chtgkey 
        FROM CHTEXTUREGRP 
       WHERE rvindicator = 'Yes'
       GROUP BY chkey) 
      AS b ON a.chtgkey = b.chtgkey
INNER JOIN (
      SELECT x.* 
        FROM chtexture AS x
       INNER JOIN (
             SELECT chtgkey, min(chtkey) AS chtkey 
               FROM  chtexture
              GROUP BY chtgkey) 
             AS y ON x.chtkey = y.chtkey)
      AS c ON a.chtgkey = c.chtgkey;"""
]

postgis_views = [
"""/* This query returns the surface soil horizon and should be joined 
to the component or qry_component_dominant via the cokey in arcGIS */
CREATE OR REPLACE VIEW chorizon_surface AS
SELECT * 
  FROM CHORIZON 
 WHERE hzdept_r = 0;""",
 
"""/* This selects the top layer from chorizon that is not organic material */
CREATE OR REPLACE VIEW chorizon_surface_noOM AS
SELECT * 
  FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY hzdept_r) AS RowNum 
		  FROM chorizon 
		 WHERE desgnmaster != 'O'
	    ) AS a
 WHERE RowNum = 1;""",
 
 """/* View isolates only one record in the case of multiple records per soil horizon
join to chorizon or qry_chorizon_surface via chkey */
CREATE OR REPLACE VIEW chunified_first AS
SELECT a.unifiedcl, a.rvindicator, a.chkey, a.chunifiedkey 
  FROM (SELECT *, Row_Number() OVER (PARTITION BY chkey ORDER BY rvindicator DESC, chunifiedkey) AS RowNum 
	      FROM chunified
       ) AS a 
 WHERE RowNum = 1;""",
 
 """/* Isolates one ecosite per component by ecoclasstype in the case of multiple ecosites per component 
(in order of ecoclasstype abundance within the database). Join to component or qry_component_dominant via cokey. */
CREATE OR REPLACE VIEW coecoclass_codominant AS
SELECT y.*, 
       CASE WHEN SUBSTRING(ecoclassid, 1,1) IN ('F','R') THEN SUBSTRING(ecoclassid, 2, 10) 
            WHEN SUBSTRING(ecoclassid, 1,1) = '0' THEN SUBSTRING(ecoclassid, 1, 10) 
            ELSE ecoclassid END AS ecoclassid_std, 
       LOWER(LTRIM(RTRIM(REPLACE(REPLACE(ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std 
  FROM coecoclass AS y 
 INNER JOIN (
       SELECT cokey, coecoclasskey 
	     FROM (SELECT a.cokey, a.coecoclasskey, b.n, ROW_NUMBER() OVER(PARTITION BY a.cokey ORDER BY b.n DESC) AS RowNum 
			     FROM coecoclass AS a 
			    INNER JOIN (
				      SELECT ecoclasstypename, Count(cokey) AS n 
						FROM coecoclass 
					   GROUP BY ecoclasstypename 
					  ) AS b ON a.ecoclasstypename = b.ecoclasstypename 
			  ) AS x 
	    WHERE RowNum = 1 
	   ) AS z ON y.coecoclasskey = z.coecoclasskey;""",
	   
"""/* Identifies the dominant ecosite per map unit by summing up comppct_r values*/
CREATE OR REPLACE VIEW coecoclass_mudominant AS
SELECT mukey, ecoclassid, ecoclassid_std, ecoclassname, 
       LOWER(LTRIM(RTRIM(REPLACE(REPLACE(ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std, 
	   ecoclasspct, ecotype
  FROM (
       SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, l.ecotype, g.ecoclasspct, 
	      ROW_NUMBER() OVER(PARTITION BY g.mukey ORDER BY g.ecoclasspct Desc, g.ecoclassid) AS RowNum
	     FROM (
	          SELECT q.mukey, COALESCE(r.ecoclassid, 'NA') AS ecoclassid, Sum(q.comppct_r) AS ecoclasspct
		        FROM COMPONENT AS q
		        LEFT JOIN (
			              SELECT y.*
			              FROM coecoclass AS y 
			              INNER JOIN (
				                     SELECT cokey, coecoclasskey
				                       FROM (
					                        SELECT a.cokey, a.coecoclasskey, b.n, 
						                           ROW_NUMBER() OVER (PARTITION BY a.cokey ORDER BY b.n DESC) AS RowNum
					                          FROM coecoclass AS a 
					                         INNER JOIN (
						                                SELECT ecoclasstypename, Count(cokey) AS n
						                                  FROM coecoclass
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
			              CASE WHEN SUBSTRING(ecoclassid, 1, 1) = 'F' THEN 'forest'
			                   WHEN SUBSTRING(ecoclassid, 1, 1) = 'R' THEN 'range'
			                   ELSE NULL END AS ecotype,    
			              ecoclassname 
	                 FROM (
		                  SELECT ecoclassid, ecoclassname, n, 
					             ROW_NUMBER() OVER(PARTITION BY ecoclassid ORDER BY n DESC, ecoclassname) AS RowNum
		                    FROM (
			                     SELECT ecoclassid, ecoclassname, Count(ecoclassname) AS n
			                       FROM coecoclass
			                      GROUP BY ecoclassid, ecoclassname
		                         ) AS j
	                      ) AS k
	                WHERE RowNum = 1
			        ) AS l ON g.ecoclassid = l.ecoclassid
       ) AS m
 WHERE RowNum = 1;""",
 
 """/* Gives dominant and maximum flooding/ponding frequency class and flooding/ponding frequency duration grouped by comonth.
Join to comonent or qry_component_dominant via cokey. */
CREATE OR REPLACE VIEW comonth_flodpondfreqcl AS
SELECT a.cokey, 
       b.flodfreqcl AS flodfreqcl_max, d.flodfreqcl AS flodfreqcl_dom, d.flodfreqcl_n AS flodfreqcl_dom_n,
       c.floddurcl AS floddurcl_max, e.floddurcl AS floddurcl_dom, e.floddurcl_n AS floddurcl_dom_n,
       f.pondfreqcl AS pondfreqcl_max, h.pondfreqcl AS pondfreqcl_dom, h.pondfreqcl_n AS pondfreqcl_dom_n,
       g.ponddurcl AS ponddurcl_max, i.ponddurcl AS ponddurcl_dom, i.ponddurcl_n AS ponddurcl_dom_n
  FROM (SELECT cokey FROM comonth GROUP BY cokey) AS a
  --flooding section
  LEFT JOIN (
       SELECT cokey, flodfreqcl, flodfreqclno
         FROM (
              SELECT cokey, flodfreqcl, flodfreqclno, 
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY flodfreqclno DESC) AS rownum
                FROM (
                     SELECT cokey, flodfreqcl, 
                            CASE WHEN flodfreqcl IS NULL THEN NULL
                                 WHEN flodfreqcl = 'None' THEN 1 
                                 WHEN flodfreqcl = 'Rare' THEN 2 
                                 WHEN flodfreqcl = 'Occasional' THEN 3 
                                 WHEN flodfreqcl = 'Frequent' THEN 4 
                                 ELSE NULL END AS flodfreqclno
                       FROM comonth
                     ) AS bbb    
              ) AS bb
        WHERE rownum = 1
       ) AS b ON a.cokey = b.cokey
  LEFT JOIN (
       SELECT cokey, floddurcl, floddurclno
         FROM (
              SELECT cokey, floddurcl, floddurclno,
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY floddurclno DESC) AS rownum
                FROM (
                     SELECT cokey, floddurcl,           
                            CASE WHEN floddurcl IS NULL THEN NULL
                                 WHEN floddurcl = 'Extremely brief (0.1 to 4 hours)' THEN 1 
                                 WHEN floddurcl = 'Very brief (4 to 48 hours)' THEN 2 
                                 WHEN floddurcl = 'Brief (2 to 7 days)' THEN 3 
                                 WHEN floddurcl = 'Long (7 to 30 days)' THEN 4 
                                 WHEN floddurcl = 'Very long (more than 30 days)' THEN 5 
                                 ELSE NULL END AS floddurclno
                       FROM comonth
                     ) AS ccc
              ) AS cc
        WHERE rownum = 1
       ) AS c ON a.cokey = c.cokey
  LEFT JOIN (
       SELECT cokey, flodfreqcl, flodfreqcl_n
         FROM (
              SELECT cokey, flodfreqcl, flodfreqcl_n,
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY flodfreqcl_n DESC) AS rownum
                FROM (
                     SELECT cokey, flodfreqcl, count(flodfreqcl) AS flodfreqcl_n
                       FROM comonth
                      GROUP BY cokey, flodfreqcl
                     ) AS ddd 
               ) AS dd
        WHERE rownum = 1
       ) AS d ON a.cokey = d.cokey
  LEFT JOIN (
       SELECT cokey, floddurcl, floddurcl_n
         FROM (
              SELECT cokey, floddurcl, floddurcl_n,
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY floddurcl_n DESC) AS rownum
                FROM (
                     SELECT cokey, floddurcl, count(floddurcl) AS floddurcl_n    
                       FROM comonth
                      GROUP BY cokey, floddurcl
                     ) AS eee
              ) AS ee
        WHERE rownum = 1
       ) AS e ON a.cokey = e.cokey
  --ponding section 
  LEFT JOIN (
       SELECT cokey, pondfreqcl, pondfreqclno
         FROM (
              SELECT cokey, pondfreqcl, pondfreqclno, 
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY pondfreqclno DESC) AS rownum
                FROM (
                     SELECT cokey, pondfreqcl, 
                            CASE WHEN pondfreqcl IS NULL THEN NULL
                                 WHEN pondfreqcl = 'None' THEN 1 
                                 WHEN pondfreqcl = 'Rare' THEN 2 
                                 WHEN pondfreqcl = 'Occasional' THEN 3 
                                 WHEN pondfreqcl = 'Frequent' THEN 4 
                                 ELSE NULL END AS pondfreqclno
                       FROM comonth
                     ) AS fff    
              ) AS ff
        WHERE rownum = 1
       ) AS f ON a.cokey = f.cokey
  LEFT JOIN (
       SELECT cokey, ponddurcl, ponddurclno
         FROM (
              SELECT cokey, ponddurcl, ponddurclno,
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY ponddurclno DESC) AS rownum
                FROM (
                     SELECT cokey, ponddurcl,           
                            CASE WHEN ponddurcl IS NULL THEN NULL
                                 WHEN ponddurcl = 'Extremely brief (0.1 to 4 hours)' THEN 1 
                                 WHEN ponddurcl = 'Very brief (4 to 48 hours)' THEN 2 
                                 WHEN ponddurcl = 'Brief (2 to 7 days)' THEN 3 
                                 WHEN ponddurcl = 'Long (7 to 30 days)' THEN 4 
                                 WHEN ponddurcl = 'Very long (more than 30 days)' THEN 5 
                                 ELSE NULL END AS ponddurclno
                       FROM comonth
                     ) AS ggg
              ) AS gg
        WHERE rownum = 1
       ) AS g ON a.cokey = g.cokey
  LEFT JOIN (
       SELECT cokey, pondfreqcl, pondfreqcl_n
         FROM (
              SELECT cokey, pondfreqcl, pondfreqcl_n,
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY pondfreqcl_n DESC) AS rownum
                FROM (
                     SELECT cokey, pondfreqcl, count(pondfreqcl) AS pondfreqcl_n
                       FROM comonth
                      GROUP BY cokey, pondfreqcl
                     ) AS hhh 
               ) AS hh
        WHERE rownum = 1
       ) AS h ON a.cokey = h.cokey
  LEFT JOIN (
       SELECT cokey, ponddurcl, ponddurcl_n
         FROM (
              SELECT cokey, ponddurcl, ponddurcl_n,
                     ROW_NUMBER() OVER(PARTITION BY cokey ORDER BY ponddurcl_n DESC) AS rownum
                FROM (
                     SELECT cokey, ponddurcl, count(ponddurcl) AS ponddurcl_n    
                       FROM comonth
                      GROUP BY cokey, ponddurcl
                     ) AS iii
              ) AS ii
        WHERE rownum = 1
       ) AS i ON a.cokey = i.cokey;""",

"""/* Provides minimum and maximum soil moisture depth levels group by comonth.  Join to component or qry_component_dominant */
CREATE OR REPLACE VIEW comonth_soimoistdept AS
SELECT a.cokey, min(b.soimoistdept_r) AS soimoistdept_rmin, max(b.soimoistdept_r) AS soimoistdept_rmax 
  FROM comonth AS a 
 INNER JOIN cosoilmoist AS b ON a.comonthkey = b.comonthkey 
 WHERE soimoiststat = 'Wet' 
 GROUP BY a.cokey;""",

 """/* Provides the most dominant component per map unit by comppct_r (highest) and compname (descending alphabetical order) in case of tie.
Join to mupolygon or mapunit via mukey. */
CREATE OR REPLACE VIEW component_dominant AS
 SELECT a.*
  FROM component AS a 
 INNER JOIN (
	   SELECT cokey, ROW_NUMBER() OVER (PARTITION BY mukey ORDER BY comppct_r DESC, compname ASC) AS RowNum
	     FROM component
       ) AS b ON a.cokey = b.cokey
 WHERE b.RowNum = 1;""",
 
 """/* In the case that there is more than one texture available per horizon, returns the one with the alphabetically lowest chkey.
Join to chorizon or qry_chorizon_surface via chkey. */
CREATE OR REPLACE VIEW texture_first AS
 SELECT a.*, c.texcl, c.lieutex, c.chtkey FROM chtexturegrp AS a
 INNER JOIN (
	   SELECT chkey, Min(chtgkey) AS chtgkey 
	     FROM chtexturegrp 
	    WHERE rvindicator = 'Yes'
	    GROUP BY chkey
       ) AS b ON a.chtgkey = b.chtgkey
 INNER JOIN (
	   SELECT x.* 
	     FROM chtexture AS x
	    INNER JOIN (
		      SELECT chtgkey, Min(chtkey) AS chtkey 
		        FROM chtexture
		       GROUP BY chtgkey
	          ) AS y ON x.chtkey = y.chtkey
       ) AS c ON a.chtgkey = c.chtgkey;"""
]
