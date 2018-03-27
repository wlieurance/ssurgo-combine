import os
import csv
import sys
import argparse
import sqlite3 as sqlite
import psycopg2
from config import spatialite_tables, postgis_tables

ecogroup_spatialite = [
"""/* This  view  determines the dominant ecogroup per map unit from values listed in the ecogroup table. */
CREATE VIEW IF NOT EXISTS ecogroup_mudominant AS
SELECT y.mukey, y.ecogroup, y.group_type, y.modal, y.pub_status, max(y.ecogrouppct) AS ecogrouppct
  FROM (
       SELECT x.mukey, x.ecogroup, x.group_type, x.modal, x.pub_status, sum(x.ecoclasspct) AS ecogrouppct
         FROM (
              SELECT a.mukey, 
                     CASE WHEN b.ecoid IS NULL THEN a.ecoclassid_std ELSE b.ecogroup END AS ecogroup,
                     CASE WHEN b.ecoid IS NULL THEN CASE WHEN a.ecoclassid_std IS NULL THEN NULL 
                                                         ELSE 'ecosite' END 
                          ELSE b.group_type END AS group_type,
                     CASE WHEN b.ecoid IS NULL THEN CASE WHEN a.ecoclassid_std IS NULL THEN NULL
                                                         ELSE 1 END 
                          ELSE b.modal END AS modal,
                     b.pub_status, a.ecoclasspct
                FROM coecoclass_mudominant AS a
                LEFT JOIN ecogroup AS b ON a.ecoclassid_std = b.ecoid)
              AS x
        GROUP BY x.mukey, x.ecogroup, x.group_type, x.modal)
       AS y
 GROUP BY mukey;""",

"""SELECT DiscardGeometryColumn('ecogrouppolygon', 'shape');""",

"""DROP TABLE IF EXISTS ecogrouppolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecogroup polygons. */
CREATE TABLE IF NOT EXISTS ecogrouppolygon (
       OBJECTID {oid}, 
       ecogroup {limit_text} (50),
       group_type {limit_text} (50),
       modal {bool},
       pub_status {limit_text} (20),
       area_dd {double},
       ecogrouppct {double});""",

"""SELECT AddGeometryColumn('ecogrouppolygon', 'shape', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecogroup per polygon with area percentage of ecogroup. Inserted into table for usefulness and speed. */
INSERT INTO ecogrouppolygon (ecogroup, group_type, modal, pub_status, area_dd, ecogrouppct, shape)
SELECT ecogroup, group_type, modal, pub_status, ST_Area(shape) AS area_dd, (ecogrouparea_dd/ST_Area(shape)) AS ecogrouppct, ST_Multi(shape) AS shape
  FROM (
       SELECT ST_Union(shape) AS shape, ecogroup, group_type, modal, pub_status, sum(ecogrouparea_dd) AS ecogrouparea_dd
         FROM (
              SELECT a.shape, b.ecogroup, b.group_type, b.modal, b.pub_status,
                     (ST_Area(a.shape)*(CAST(b.ecogrouppct AS REAL)/100)) AS ecogrouparea_dd
                FROM mupolygon AS a
                LEFT JOIN ecogroup_mudominant AS b ON a.mukey = b.mukey)
              AS x
        GROUP BY ecogroup) AS y;"""
]

ecogroup_postgis = [
"""/* This  view  determines the dominant ecogroup per map unit from values listed in the ecogroup table. */
CREATE OR REPLACE VIEW ecogroup_mudominant AS
SELECT f.mukey, f.ecogroup, f.group_type, f.modal, f.pub_status, f.ecogrouppct 
  FROM (SELECT Row_Number() OVER (PARTITION BY b.mukey ORDER BY b.ecogrouppct DESC, b.group_type ASC, b.ecogroup ASC) AS rownum, b.mukey, b.ecogroup, b.group_type, b.modal, b.pub_status, b.ecogrouppct 
	  FROM (SELECT a.mukey, a.ecogroup, a.group_type, a.pub_status, a.modal, sum(a.ecoclasspct) AS ecogrouppct 
		  FROM (SELECT m.mukey, m.ecoclassid, m.ecoclassid_std, m.ecoclassname, LOWER(LTRIM(RTRIM(REPLACE(REPLACE(m.ecoclassname,'"',' in'),'  ',' ')))) AS ecoclassname_std, m.ecoclasspct,
			       CASE WHEN m.ecoclassid_std IS NULL THEN Null
				    WHEN n.ecogroup IS NULL THEN m.ecoclassid_std 
				    ELSE n.ecogroup END AS ecogroup, 
			       CASE WHEN m.ecoclassid_std IS NULL THEN Null
				    WHEN n.ecogroup IS NULL THEN (CASE WHEN LEFT(m.ecoclassid, 1) = 'R' THEN 'Range'
                                                                       WHEN LEFT(m.ecoclassid, 1) = 'F' THEN 'Forest'
				                                       ELSE Null END)
				    ELSE Null END AS ecosubgroup,
				    CASE WHEN m.ecoclassid_std IS NULL THEN Null
				    WHEN n.ecogroup IS NULL THEN 'Ecosite'
				    ELSE n.group_type END AS group_type, 
			       CASE WHEN m.ecoclassid_std IS NULL THEN Null
				    WHEN n.ecogroup IS NULL THEN TRUE
				    ELSE n.modal END AS modal, 
			       CASE WHEN m.ecoclassid_std IS NULL THEN Null
				    ELSE n.pub_status END AS pub_status 
			  FROM (SELECT g.mukey, g.ecoclassid, l.ecoclassid_std, l.ecoclassname, g.ecoclasspct, ROW_NUMBER() OVER(PARTITION BY g.mukey ORDER BY g.ecoclasspct Desc, g.ecoclassid) AS RowNum
				  FROM (SELECT q.mukey, COALESCE(r.ecoclassid, 'NA') AS ecoclassid, Sum(q.comppct_r) AS ecoclasspct
					  FROM COMPONENT AS q
					  LEFT JOIN (SELECT y.*
						       FROM COECOCLASS AS y 
						      INNER JOIN (SELECT cokey, coecoclasskey
                                                                    FROM (SELECT a.cokey, a.coecoclasskey, b.n, ROW_NUMBER() OVER (PARTITION BY a.cokey ORDER BY b.n DESC) AS RowNum
									    FROM COECOCLASS AS a 
									   INNER JOIN (SELECT ecoclasstypename, Count(cokey) AS n
											 FROM COECOCLASS
											GROUP BY ecoclasstypename) AS b ON a.ecoclasstypename = b.ecoclasstypename) AS x
									   WHERE RowNum = 1) AS z ON y.coecoclasskey = z.coecoclasskey) AS r ON q.cokey = r.cokey
								   GROUP BY mukey, ecoclassid) AS g
						       LEFT JOIN (SELECT ecoclassid, 
								         CASE WHEN SUBSTRING(ecoclassid, 1, 1) IN ('F', 'R') THEN SUBSTRING(ecoclassid, 2, 10) 
									      WHEN SUBSTRING(ecoclassid, 1, 1) = '0' THEN SUBSTRING(ecoclassid, 1, 10) 
									      ELSE ecoclassid END AS ecoclassid_std, ecoclassname 
							            FROM (SELECT ecoclassid, ecoclassname, n, ROW_NUMBER() OVER(PARTITION BY ecoclassid ORDER BY n DESC, ecoclassname) AS RowNum
								            FROM (SELECT ecoclassid, ecoclassname, Count(ecoclassname) AS n
									            FROM COECOCLASS
									           GROUP BY ecoclassid, ecoclassname) AS j) AS k
							           WHERE RowNum = 1) AS l ON g.ecoclassid = l.ecoclassid) AS m
					              LEFT JOIN ecogroup AS n on m.ecoclassid_std = n.ecoid) AS a
             GROUP BY mukey, ecogroup, group_type, modal, pub_status) AS b) AS f
 WHERE rownum = 1;""",

"""DROP TABLE IF EXISTS ecogrouppolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecogroup polygons. */
CREATE TABLE IF NOT EXISTS ecogrouppolygon (
       OBJECTID {oid}, 
       ecogroup {limit_text} (50),
       group_type {limit_text} (50),
       modal {bool},
       pub_status {limit_text} (20),
       area_dd {double},
       ecogrouppct {double});""",

"""SELECT AddGeometryColumn('ecogrouppolygon', 'shape', 4326, 'MULTIPOLYGON', 2);""",

"""/* Spatial view showing dominant ecogroup per polygon with area percentage of ecogroup. Inserted into table for usefulness and speed. */
INSERT INTO ecogrouppolygon (ecogroup, group_type, modal, pub_status, area_dd, ecogrouppct, shape)
SELECT ecogroup, group_type, modal, pub_status, ST_Area(shape) AS area_dd, (ecogrouparea_dd/ST_Area(shape)) AS ecogrouppct, ST_Multi(shape) AS shape
  FROM (
       SELECT ST_Union(shape) AS shape, ecogroup, group_type, modal, pub_status, sum(ecogrouparea_dd) AS ecogrouparea_dd
         FROM (
              SELECT a.shape, b.ecogroup, b.group_type, b.modal, b.pub_status,
                     (ST_Area(a.shape)*(CAST(b.ecogrouppct AS REAL)/100)) AS ecogrouparea_dd
                FROM mupolygon AS a
                LEFT JOIN ecogroup_mudominant AS b ON a.mukey = b.mukey)
              AS x
        GROUP BY ecogroup, group_type, modal, pub_status) AS y;"""
]

def create_table(dbpath, dbtype):
    sql = ("CREATE TABLE IF NOT EXISTS ecogroup (ecoid {limit_text} (20) PRIMARY KEY, ecogroup {limit_text} (50) NOT NULL, group_type {limit_text} "
           "(50), modal {bool}, pub_status {limit_text} (20));")
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        sql = sql.format(**spatialite_tables)
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        sql = sql.format(**postgis_tables)
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    conn.close()

def load_ecogroups(dbpath, dbtype, csvpath):
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
    with open(csvpath, 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        headers = next(csvreader)
        SQL = "INSERT {{!s}} INTO ecogroup (ecoid, ecogroup, group_type, modal, pub_status) VALUES ({!s}) {{!s}};".format(','.join([paramstr]*5))
        SQL = SQL.format(ins1, ins2)
        for row in csvreader:
            convert = []
            for r in row:
                if r:
                    convert.append(r)
                else:
                    convert.append(None)
            
            c.execute(SQL,(convert[0], convert[1], convert[2], convert[3], convert[4]))
    conn.commit()
    conn.close()

def create_views(dbpath, dbtype):
    print("Creating ecogroup objects...")
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('mod_spatialite');")
        c = conn.cursor()
        for stmt in ecogroup_spatialite:
            c.execute(stmt.format(**spatialite_tables))
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        for stmt in ecogroup_postgis:
            c.execute(stmt.format(**postgis_tables))
    conn.commit()
    conn.close()



    
if __name__ == "__main__":
    ### parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will add ecological groupings to an already created SSURGO database. '
                                     'The ecogroups table will be created and populated via a comma delimited file, and the '
                                     'ecogrouppolygon spatial feature will be created.')
    ### positional arguments
    parser.add_argument('dbpath', help='path of the sqlite SSURGO created via main.py within this toolset')
    parser.add_argument('csvpath', help='path of the csv file containing the ecological groupings (see ecogroups_example.csv)')
    parser.add_argument('-t', '--type', metavar='DATABASE_TYPE', default = 'spatialite', 
                        help='decides which database type to import to, spatialite or postgis')
    args = parser.parse_args()
    ### check for valid arguments
    if args.type == 'spatialite':
        if not os.path.isfile(args.dbpath):
            print("dbpath does not exist. Please choose an existing SSURGO file to use.")
            quit()
    if not os.path.isfile(args.csvpath):
        print("csvpath does not exist. Please choose an existing comma delimited ecogroup file to use.")
        quit()
    
    create_table(args.dbpath, args.type)
    load_ecogroups(args.dbpath, args.type, args.csvpath)
    create_views(args.dbpath, args.type)
    print('Script finished.')
