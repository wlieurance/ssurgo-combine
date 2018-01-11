import os
import csv
import sys
import sqlite3 as sqlite

ecogroup_statements = [
"""/* This  view  determines the dominant ecogroup per map unit from values listed in the ecogroups table. */
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
                LEFT JOIN ecogroups AS b ON a.ecoclassid_std = b.ecoid)
              AS x
        GROUP BY x.mukey, x.ecogroup, x.group_type, x.modal)
       AS y
 GROUP BY mukey;""",

"""DROP TABLE IF EXISTS ecogrouppolygon;""",

"""/* Creates a new table in which store spatial query results for dominant ecogroup polygons. */
CREATE TABLE IF NOT EXISTS ecogrouppolygon (
       OBJECTID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
       ecogroup TEXT (50),
       group_type TEXT (50),
       modal INTEGER,
       pub_status TEXT (20),
       area_ha FLOAT64,
       ecogrouppct FLOAT64);""",

"""SELECT AddGeometryColumn('ecogrouppolygon', 'SHAPE', 4326, 'MULTIPOLYGON', 'XY');""",

"""/* Spatial view showing dominant ecogroup per polygon with area percentage of ecogroup. Inserted into table for usefulness and speed. */
INSERT INTO ecogrouppolygon (ecogroup, group_type, modal, pub_status, area_ha, ecogrouppct, SHAPE)
SELECT ecogroup, group_type, modal, pub_status, (ST_Area(SHAPE)/10000) AS area_ha, (ecogrouparea_sqm/ST_Area(SHAPE)) AS ecogrouppct, ST_Multi(SHAPE) AS SHAPE
  FROM (
       SELECT ST_Union(SHAPE) AS SHAPE, ecogroup, group_type, modal, pub_status, sum(ecogrouparea_sqm) AS ecogrouparea_sqm
         FROM (
              SELECT a.SHAPE, b.ecogroup, b.group_type, b.modal, b.pub_status,
                     (ST_Area(a.SHAPE)*(CAST(b.ecogrouppct AS REAL)/100)) AS ecogrouparea_sqm
                FROM mupolygon AS a
                LEFT JOIN ecogroup_mudominant AS b ON a.mukey = b.mukey)
              AS x
        GROUP BY ecogroup) AS y;"""
]

def printhelp():
    print("\nThis script will load custom ecological groupings into an existing SpatialLite SSURGO created with the import.py script. "
          "The script will load them from a comma delimited file into a new custom table called 'ecogroups' and create an associated "
          "polygon feature called 'ecogrouppolygon' which sptially displays dominant ecogroups. See the 'example.csv' file for a template.")
    print("\nUsage syntax: create_ecogroups.py dbfile csvfile\n")

def create_table(dbpath):
    conn = sqlite.connect(dbpath)
    c = conn.cursor()
    sql = "CREATE TABLE IF NOT EXISTS ecogroups (ecoid TEXT (20) PRIMARY KEY, ecogroup TEXT (50) NOT NULL, group_type TEXT (50), modal INTEGER, pub_status TEXT (20));"
    c.execute(sql)
    conn.commit()
    conn.close()

def load_ecogroups(dbpath, csvpath):
    conn = sqlite.connect(dbpath)
    c = conn.cursor()
    with open(csvpath, 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        headers = next(csvreader)
        for row in csvreader:
            convert = []
            for r in row:
                if r:
                    convert.append(r)
                else:
                    convert.append(None)
            c.execute("INSERT OR IGNORE INTO ecogroups (ecoid, ecogroup, group_type, modal, pub_status) VALUES (?,?,?,?,?);",
                      (convert[0], convert[1], convert[2], convert[3], convert[4]))
    conn.commit()
    conn.close()

def create_views(dbpath):
    conn = sqlite.connect(dbpath)
    conn.enable_load_extension(True)
    c = conn.cursor()
    c.execute("SELECT load_extension('mod_spatialite');")
    print("Creating ecogroup objects...")
    for stmt in ecogroup_statements:
        c.execute(stmt)
    conn.commit()
    conn.close()



    
if __name__ == "__main__":
    ### parses script arguments
    if len(sys.argv) > 1 and len(sys.argv) < 4:
        if str(sys.argv[1]) == '-h' or str(sys.argv[1]) == '--help':
            printhelp()
            quit()
        else:
            dbpath = os.path.abspath(str(sys.argv[1]))
            csvpath = os.path.abspath(str(sys.argv[2]))
    else:
        printhelp()
        quit()
    if not os.path.isfile(dbpath):
        print("dbpath does not exist. Please choose an existing SSURGO file to use.")
        quit()
    if not os.path.isfile(csvpath):
        print("csvpath does not exist. Please choose an existing comma delimited ecogroup file to use.")
        quit()

    create_table(dbpath)
    load_ecogroups(dbpath, csvpath)
    create_views(dbpath)
    print('Script finished.')
