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
      AS c ON a.chtgkey = c.chtgkey;""",
	  
"""/* Compiles a list of unique components in the database and displays some statistics, including total area in acres
   Calculated from the mupolygon feature and the comppct_r field. */
CREATE VIEW IF NOT EXISTS component_unique AS
SELECT x.compname, MIN(x.compkind) AS compkind, 
       AVG(CAST(x.comppct_r AS FLOAT)) AS comppct_r_mean, SUM(x.compacres) AS compacres
  FROM ( 
	   SELECT a.mukey, a.compname, a.compkind, a.comppct_r,
			 (a.comppct_r * b.muacres / 100) AS compacres
		 FROM component AS a
		 LEFT JOIN (
			  SELECT mukey, ST_Area(ST_Union(shape), 1) * 0.000247105 AS muacres
				FROM mupolygon
			   GROUP BY mukey
			  ) AS b ON a. mukey = b.mukey
	   ) AS x
 GROUP BY x.compname;"""
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
       ) AS c ON a.chtgkey = c.chtgkey;""",
	   
"""/* Creates a list of ecosites per map unit key ranked by total area */
CREATE OR REPLACE VIEW coecoclass_mapunit_ranked AS
SELECT mukey, ecoclassid, ecoclassid_std, ecoclassname, 
       LOWER(LTRIM(RTRIM(REPLACE(REPLACE(ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std, 
	   ecotype, ecoclasspct, ecorank
  FROM (
	   SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, l.ecotype, g.ecoclasspct, 
	          ROW_NUMBER() OVER(PARTITION BY g.mukey ORDER BY g.ecoclasspct Desc, g.ecoclassid) AS ecorank
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
       ) AS m;""",
	   
"""/* Identifies the dominant ecosite per map unit by summing up comppct_r values*/
CREATE OR REPLACE VIEW coecoclass_mudominant AS
SELECT mukey, ecoclassid, ecoclassid_std, ecoclassname, ecoclassname_std, ecotype, ecoclasspct 
  FROM coecoclass_mapunit_ranked
 WHERE ecorank = 1;""",

"""/* Isolates the six most dominant ecosites per mapunit and arranges them in a wide table format with their percentages.
Join to mupolygon or mapunit via mukey */
CREATE OR REPLACE VIEW coecoclass_wide AS
SELECT n.mukey, 
       n.ecoclassid AS ecoclassid_1, n.ecoclassname AS ecoclassname_1, n.ecoclasspct AS ecoclasspct_1, 
       o.ecoclassid AS ecoclassid_2, o.ecoclassname AS ecoclassname_2, o.ecoclasspct AS ecoclasspct_2, 
	   p.ecoclassid AS ecoclassid_3, p.ecoclassname AS ecoclassname_3, p.ecoclasspct AS ecoclasspct_3, 
	   q.ecoclassid AS ecoclassid_4, q.ecoclassname AS ecoclassname_4, q.ecoclasspct AS ecoclasspct_4, 
	   r.ecoclassid AS ecoclassid_5, r.ecoclassname AS ecoclassname_5, r.ecoclasspct AS ecoclasspct_5, 
	   s.ecoclassid AS ecoclassid_6, s.ecoclassname AS ecoclassname_6, s.ecoclasspct AS ecoclasspct_6
  FROM coecoclass_mapunit_ranked AS n
  LEFT JOIN coecoclass_mapunit_ranked AS o ON n.mukey = o.mukey
  LEFT JOIN coecoclass_mapunit_ranked AS p ON n.mukey = p.mukey
  LEFT JOIN coecoclass_mapunit_ranked AS q ON n.mukey = q.mukey
  LEFT JOIN coecoclass_mapunit_ranked AS r ON n.mukey = r.mukey
  LEFT JOIN coecoclass_mapunit_ranked AS s ON n.mukey = s.mukey
 WHERE n.ecorank = 1 AND 
       o.ecorank = 2 AND 
	   p.ecorank = 3 AND 
	   q.ecorank = 4 AND 
	   r.ecorank = 5 AND 
	   s.ecorank = 6;""",
       
""" /* Creates a unique list of ecosites in the database and calculates area statistics via mupolygon.shape and component.comppct_r (ecoacres).
       ecoclasspct_mean is the average percentage of the a map unit the ecosite takes up if it is present. */
CREATE OR REPLACE VIEW coecoclass_unique AS
SELECT COALESCE(i.ecoclassid_std, 'NA') AS ecoclassid_std, 
       j.ecoclassid, j.ecoclassname, j.ecoclassname_std, j.ecoclasstypename, i.eco_n, i.ecoclasspct_mean, i.ecoacres
  FROM (
		SELECT x.ecoclassid_std, COUNT(x.ecoclassid_std) AS eco_n,
			   AVG(CAST(ecoclasspct AS float)) AS ecoclasspct_mean, SUM(ecoacres) AS ecoacres
		  FROM (
				SELECT a.mukey, a.ecoclassid, a.ecoclassid_std, a.ecoclassname, a.ecoclassname_std, 
					   a.ecotype, a.ecoclasspct, a.ecorank, (a.ecoclasspct * b.muacres / 100) AS ecoacres
				  FROM coecoclass_mapunit_ranked AS a
				  LEFT JOIN (
					   SELECT mukey, ST_Area(ST_Union(shape), true) * 0.000247105 AS muacres
						 FROM mupolygon
						GROUP BY MUKEY
					   ) AS b ON a.mukey = b.mukey
				) AS x
		 GROUP BY x.ecoclassid_std) AS i
  LEFT JOIN (
			SELECT h.ecoclasstypename, h.ecoclassid_std, h.ecoclassid, h.ecoclassname,
				   LOWER(LTRIM(RTRIM(REPLACE(REPLACE(h.ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std 
			  FROM (SELECT g.ecoclasstypename, g.ecoclassid_std, g.ecoclassid, g.ecoclassname, g.pref_order,
						   ROW_NUMBER() OVER(PARTITION BY g.ecoclassid_std ORDER BY g.pref_order) AS preference
					  FROM (SELECT f.ecoclasstypename, f.ecoclassid_std, f.ecoclassid, f.ecoclassname, f.pref_order
							  FROM (SELECT x.ecoclasstypename, x.ecoclassid, 
										   CASE WHEN Substring(ecoclassid,1,1) = '0' THEN Substring(ecoclassid,1,10)
												WHEN Substring(ecoclassid,1,1) IN ('R','F') THEN Substring(ecoclassid,2,10)
												ELSE NULL END AS ecoclassid_std,
										   x.ecoclassname, y.pref_order
									  FROM coecoclass AS x
									 INNER JOIN (SELECT a.ecoclasstypename, a.n, ROW_NUMBER() OVER(ORDER BY a.n DESC) AS pref_order 
												   FROM (SELECT ecoclasstypename, Count(ecoclasstypename) AS n 
														   FROM coecoclass 
														  GROUP BY ecoclasstypename) AS a) AS y ON x.ecoclasstypename = y.ecoclasstypename) AS f
							 GROUP BY f.ecoclasstypename, f.ecoclassid_std, f.ecoclassid, f.ecoclassname, f.pref_order) AS g) AS h
			 WHERE h.preference = 1 AND 
				   LOWER(h.ecoclassid) != 'null' AND 
				   h.ecoclassid IS NOT NULL AND
				   POSITION('?' IN h.ecoclassid) = 0
			) AS j ON i.ecoclassid_std = j.ecoclassid_std;""",
	   
"""/* Creates a list of components per map unit key ranked by total area */
CREATE OR REPLACE VIEW component_mapunit_ranked AS
SELECT a.mukey, a.cokey, a.compname, 
       CONCAT(a.compname,
	          CASE WHEN a.localphase IS NULL THEN '' 
	               ELSE ' ' || a.localphase END,
	          CASE WHEN c.texdesc IS NULL THEN '' 
				   ELSE ' ' || LOWER(c.texdesc) END,
			  CASE WHEN a.slope_l IS NULL THEN '' 
				   ELSE CONCAT(', ',
				               CAST(a.slope_l AS Int),
							   ' to ',
							   CAST(a.slope_h AS Int),
							   '% slope') 
							   END) 
	   AS compnamelong, 
	   a.comppct_r, a.comprank 
  FROM (
	   SELECT mukey, cokey, compname, comppct_r, localphase, slope_l, slope_h, 
	          ROW_NUMBER() OVER (PARTITION BY mukey ORDER BY comppct_r DESC, compname ASC) AS comprank
		 FROM component) AS a
	     LEFT JOIN (SELECT * FROM chorizon WHERE hzdept_r = 0) as b ON a.cokey = b.cokey
	     LEFT JOIN chtexturegrp AS c ON b.chkey = c.chkey;""",
		 
"""/* Provides the most dominant component per map unit by comppct_r and descending alphabetical order.
Join to mupolygon or mapunit via mukey. */
CREATE OR REPLACE VIEW component_dominant AS
SELECT mukey, cokey, compname, compnamelong, comppct_r
  FROM component_mapunit_ranked AS a 
 WHERE comprank = 1;""",
		 
"""/* Isolates the six most dominant components per mapunit and arranges them in a wide table format with their percentages
Also renames components by adding local phase, surface texture, and slope range.  Join to mupolygon or mapunit via mukey */
CREATE OR REPLACE VIEW component_wide AS
SELECT q.mukey,
       q.cokey AS cokey_1, q.compname AS compname_1, q.compnamelong AS compnamelong_1, q.comppct_r AS comppct_r_1, 
       r.cokey AS cokey_2, r.compname AS compname_2, r.compnamelong AS compnamelong_2, r.comppct_r AS comppct_r_2, 
	   s.cokey AS cokey_3, s.compname AS compname_3, s.compnamelong AS compnamelong_3, s.comppct_r AS comppct_r_3, 
	   t.cokey AS cokey_4, t.compname AS compname_4, t.compnamelong AS compnamelong_4, t.comppct_r AS comppct_r_4, 
	   u.cokey AS cokey_5, u.compname AS compname_5, u.compnamelong AS compnamelong_5, u.comppct_r AS comppct_r_5, 
	   v.cokey AS cokey_6, v.compname AS compname_6, v.compnamelong AS compnamelong_6, v.comppct_r AS comppct_r_6 
  FROM component_mapunit_ranked AS q
  LEFT JOIN component_mapunit_ranked AS r ON q.mukey = r.mukey
  LEFT JOIN component_mapunit_ranked AS s ON q.mukey = s.mukey
  LEFT JOIN component_mapunit_ranked AS t ON q.mukey = t.mukey
  LEFT JOIN component_mapunit_ranked AS u ON q.mukey = u.mukey
  LEFT JOIN component_mapunit_ranked AS v ON q.mukey = v.mukey
 WHERE q.comprank = 1 AND 
       r.comprank = 2 AND 
	   s.comprank = 3 AND 
	   t.comprank = 4 AND 
	   u.comprank = 5 AND 
	   v.comprank = 6;""",
	   
"""/* Compiles a list of unique components in the database and displays some statistics, including total area in acres
   Calculated from the mupolygon feature and the comppct_r field. */
CREATE OR REPLACE VIEW component_unique AS
SELECT x.compname, MIN(x.compkind) AS compkind, 
       AVG(CAST(x.comppct_r AS FLOAT)) AS comppct_r_mean, SUM(x.compacres) AS compacres
  FROM ( 
	   SELECT a.mukey, a.compname, a.compkind, a.comppct_r,
			 (a.comppct_r * b.muacres / 100) AS compacres
		 FROM component AS a
		 LEFT JOIN (
			  SELECT mukey, ST_Area(ST_Union(shape), true) * 0.000247105 AS muacres
				FROM mupolygon
			   GROUP BY MUKEY
			  ) AS b ON a. mukey = b.mukey
	   ) AS x
 GROUP BY x.compname;"""	   
]
