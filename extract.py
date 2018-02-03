#!/usr/bin/env python3
import os
import csv
import sys
import re
import sqlite3 as sqlite
import argparse
import subprocess
from create_tables import table_statements
from create_views import view_statements
from create_custom import custom_statements
import create_ecogroups
from create_ecogroups import ecogroup_statements

def initdb(dbpath):
    ### determines if db needs to be initialized.
    if os.path.isfile(dbpath):
        newdb = False
    else:
        newdb = True
    ### connect to database, initialize if necessary and create tables.
    conn = sqlite.connect(dbpath)
    conn.enable_load_extension(True)
    c = conn.cursor()
    c.execute("SELECT load_extension('mod_spatialite');")
    if newdb:
        print("Creating tables...")
        c.execute("SELECT InitSpatialMetaData(1);")
        for stmt in table_statements:
            c.execute(stmt)
        print("Creating views...")
        for stmt in view_statements:
            c.execute(stmt)
    conn.close()

### sets the the field size limit to as large as possible in order to deal with the legentext entries, which are large ungainly strings
def set_csv_limit():
    maxInt = sys.maxsize
    decrement = True
    while decrement:
        # decrease the maxInt value by factor 10 
        # as long as the OverflowError occurs.
        decrement = False
        try:
            csv.field_size_limit(maxInt)
        except OverflowError:
            maxInt = int(maxInt/10)
            decrement = True

def scan_insert(dbpath, scanpath, snap, survey_areas):
    ### define variables
    tablist = [('ccancov.txt', 'cocanopycover'),
               ('ccrpyd.txt', 'cocropyld'),
               ('cdfeat.txt', 'codiagfeatures'),
               ('cecoclas.txt', 'coecoclass'),
               ('ceplants.txt', 'coeplants'),
               ('cerosnac.txt', 'coerosionacc'),
               ('cfprod.txt', 'coforprod'),
               ('cfprodo.txt', 'coforprodo'),
               ('cgeomord.txt', 'cogeomordesc'),
               ('chaashto.txt', 'chaashto'),
               ('chconsis.txt', 'chconsistence'),
               ('chdsuffx.txt', 'chdesgnsuffix'),
               ('chfrags.txt', 'chfrags'),
               ('chorizon.txt', 'chorizon'),
               ('chpores.txt', 'chpores'),
               ('chstr.txt', 'chstruct'),
               ('chstrgrp.txt', 'chstructgrp'),
               ('chtexgrp.txt', 'chtexturegrp'),
               ('chtexmod.txt', 'chtexturemod'),
               ('chtext.txt', 'chtext'),
               ('chtextur.txt', 'chtexture'),
               ('chunifie.txt', 'chunified'),
               ('chydcrit.txt', 'cohydriccriteria'),
               ('cinterp.txt', 'cointerp'),
               ('cmonth.txt', 'comonth'),
               ('comp.txt', 'component'),
               ('cpmat.txt', 'copm'),
               ('cpmatgrp.txt', 'copmgrp'),
               ('cpwndbrk.txt', 'copwindbreak'),
               ('crstrcts.txt', 'corestrictions'),
               ('csfrags.txt', 'cosurffrags'),
               ('csmoist.txt', 'cosoilmoist'),
               ('csmorgc.txt', 'cosurfmorphgc'),
               ('csmorhpp.txt', 'cosurfmorphhpp'),
               ('csmormr.txt', 'cosurfmorphmr'),
               ('csmorss.txt', 'cosurfmorphss'),
               ('cstemp.txt', 'cosoiltemp'),
               ('ctext.txt', 'cotext'),
               ('ctreestm.txt', 'cotreestomng'),
               ('ctxfmmin.txt', 'cotaxfmmin'),
               ('ctxfmoth.txt', 'cotxfmother'),
               ('ctxmoicl.txt', 'cotaxmoistcl'),
               ('distimd.txt', 'distinterpmd'),
               ('distlmd.txt', 'distlegendmd'),
               ('distmd.txt', 'distmd'),
               ('lareao.txt', 'laoverlap'),
               ('legend.txt', 'legend'),
               ('ltext.txt', 'legendtext'),
               ('mapunit.txt', 'mapunit'),
               ('msdomdet.txt', 'mdstatdomdet'),
               ('msdommas.txt', 'mdstatdommas'),
               ('msidxdet.txt', 'mdstatidxdet'),
               ('msidxmas.txt', 'mdstatidxmas'),
               ('msrsdet.txt', 'mdstatrshipdet'),
               ('msrsmas.txt', 'mdstatrshipmas'),
               ('mstab.txt', 'mdstattabs'),
               ('mstabcol.txt', 'mdstattabcols'),
               ('muaggatt.txt', 'muaggatt'),
               ('muareao.txt', 'muaoverlap'),
               ('mucrpyd.txt', 'mucropyld'),
               ('mutext.txt', 'mutext'),
               ('sacatlog.txt', 'sacatalog'),
               ('sainterp.txt', 'sainterp'),
               ('sdvalgorithm.txt', 'sdvalgorithm'),
               ('sdvattribute.txt', 'sdvattribute'),
               ('sdvfolder.txt', 'sdvfolder'),
               ('sdvfolderattribute.txt', 'sdvfolderattribute'),
               ('version.txt', 'version')]

    if snap == 0:
        geom = "ST_Multi(ST_Union(Geometry))"
        print("Importing...")
    else:
        geom = "ST_SnapToGrid(ST_Multi(ST_Union(Geometry)), {!s})".format('{:.20f}'.format(snap/111319.9)) #converts meters to decimal degrees and formats for non-scientific notation
        print("Importing with snapping...")
        
    soilmu_a_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY").format(geom)

    soilmu_l_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY").format(geom)

    soilmu_p_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY").format(geom)

    soilsa_a_sql =("SELECT {!s}, AREASYMBOL, SPATIALVER, LKEY"
                   "  FROM {{!s}}"
                   " GROUP BY AREASYMBOL, SPATIALVER, LKEY").format(geom)

    soilsf_l_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY").format(geom)

    soilsf_p_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY").format(geom)

    spatlist = [('soilmu_a', 'mupolygon', 'SHAPE, areasymbol, spatialver, musym, mukey', soilmu_a_sql),
                ('soilmu_l', 'muline', 'SHAPE, areasymbol, spatialver, musym, mukey', soilmu_l_sql),
                ('soilmu_p', 'mupoint', 'SHAPE, areasymbol, spatialver, musym, mukey', soilmu_p_sql),
                ('soilsa_a', 'sapolygon', 'SHAPE, areasymbol, spatialver, lkey', soilsa_a_sql),
                ('soilsf_l', 'featline', 'SHAPE, areasymbol, spatialver, featsym, featkey', soilsf_l_sql),
                ('soilsf_p', 'featpoint', 'SHAPE, areasymbol, spatialver, featsym, featkey', soilsf_p_sql)]

    conn = sqlite.connect(dbpath)
    conn.enable_load_extension(True)
    c = conn.cursor()
    c.execute("SELECT load_extension('mod_spatialite');")
    existing = []
    c.execute("SELECT areasymbol FROM sacatalog GROUP BY areasymbol ORDER BY areasymbol;")
    for row in c:
        existing.append(row[0].lower())
    current_ssa = ''
    new_imports = False
    ### searches scanpath for SSURGO tabular data and imports into dbpath
    for root, dirs, files in os.walk(scanpath):
        for f in files:
            if f.find('soil_metadata_') >= 0 and f.find('.txt') > 0:
                current_ssa = re.findall(r'soil_metadata_(.*?).txt',f)[0]
            if not current_ssa.lower() in existing:
                if current_ssa.lower() in survey_areas or survey_areas == '':
                    ### finds and inserts tabular data
                    if f in [(i[0]) for i in tablist]:
                        tbl = [t[1] for t in tablist if t[0] == f][0]
                        print("Importing table: ", current_ssa + ' | ' + tbl, sep = '')
                        new_imports = True
                        c.execute("SELECT * FROM {!s} LIMIT 1;".format(tbl))
                        if not tbl == 'version':
                            fields = [description[0] for description in c.description]
                            SQL = "INSERT OR IGNORE INTO {!s} VALUES ({!s});".format(tbl, ','.join('?'*len(fields)))
                        else:
                            fields = ['version']
                            SQL = "INSERT OR IGNORE INTO version VALUES (?,?,?);"
                            datatype = root.split(os.path.sep)[-1].lower()
                        with open(os.path.join(root,f), 'r') as textfile:
                            dictread = csv.DictReader(textfile, fieldnames=fields, quotechar='"', delimiter='|')
                            for row in dictread:
                                for key, value in row.items():
                                    if value == '':
                                        row[key] = None
                                #print(current_ssa, tbl, row.values(), sep = ' | ')
                                if not tbl == 'version':
                                    c.execute(SQL, list(row.values()))
                                else:
                                    c.execute(SQL, [current_ssa, datatype] + list(row.values()))
                        conn.commit()

                    ### finds and inserts spatial data
                    elif any(s in f for s in [i[0] for i in spatlist]) and os.path.splitext(f)[1] == '.shp':
                        index = [s in f for s in [i[0] for i in spatlist]].index(True)
                        sublist = spatlist[index]
                        vtable = os.path.splitext(f)[0]
                        insert_table = sublist[1]
                        insert_text = sublist[2]
                        select_stmt = sublist[3]
                        print("Importing shapefile: ", current_ssa + ' | ' + vtable, sep = '')
                        new_imports = True
                        cvtSQL = """CREATE VIRTUAL TABLE {!s} USING VirtualShape("{!s}", "UTF-8", 4326);""".format(vtable, os.path.join(root, vtable))
                        iSQL = "INSERT INTO {!s} ({!s}) {!s};".format(insert_table, insert_text, select_stmt.format(vtable))
                        #print(iSQL)
                        c.execute(cvtSQL)
                        c.execute(iSQL)
                        c.execute("DROP TABLE {!s};".format(vtable))
                        conn.commit()
            else:
                if current_ssa != '':
                    print('Already imported ', current_ssa, '. Skipping ', os.path.join(root, f), sep = '')
                    
    ### delete vestige virtual table information from shapefile import                    
    c.execute("DELETE FROM virts_geometry_columns;")
    c.execute("DELETE FROM virts_geometry_columns_auth;")
    c.execute("DELETE FROM virts_geometry_columns_field_infos;")
    c.execute("DELETE FROM virts_geometry_columns_statistics;")
    
    conn.commit()
    conn.close()
    return new_imports

def make_eco(dbpath):
    print("Creating ecopolygon feature...")
    conn = sqlite.connect(dbpath)
    conn.enable_load_extension(True)
    c = conn.cursor()
    c.execute("SELECT load_extension('mod_spatialite');")
    for stmt in custom_statements:
        c.execute(stmt)
    conn.commit()
    conn.close()

def repair_geom(dbpath, featlist):
    ### fix geometries
    conn = sqlite.connect(dbpath)
    conn.enable_load_extension(True)
    c = conn.cursor()
    c.execute("SELECT load_extension('mod_spatialite');")
    tables = [('mupolygon', 'OBJECTID, areasymbol, spatialver, musym, mukey'),
              ('muline', 'OBJECTID, areasymbol, spatialver, musym, mukey'),
              ('mupoint', 'OBJECTID, areasymbol, spatialver, musym, mukey'),
              ('sapolygon', 'OBJECTID, areasymbol, spatialver, lkey'),
              ('featline', 'OBJECTID, areasymbol, spatialver, featsym, featkey'),
              ('featpoint', 'OBJECTID, areasymbol, spatialver, featsym, featkey'),
              ('ecopolygon', 'OBJECTID, ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct'),
              ('ecogrouppolygon', 'OBJECTID, ecogroup, group_type, modal, pub_status, area_ha, ecogrouppct')]
    use_tables = [k for k in tables if k[0] in featlist] #restricts tables to only features that exist (featlist)
    
    for t in use_tables:
        print("Repairing {!s} geometries...".format(t[0]))
        c.execute("CREATE TEMP TABLE {!s}_temp AS SELECT {!s}, ST_MakeValid(SHAPE) AS SHAPE FROM {!s};".format(t[0],t[1],t[0]))
        c.execute("DELETE FROM {!s};".format(t[0]))
        c.execute("INSERT INTO {!s} SELECT * FROM {!s}_temp;".format(t[0],t[0]))
        
    conn.commit()
    conn.close()

def create_spatial_indices(dbpath, sqlite_bin, featlist):
    ### add R*Tree spatial indices.
    ##  case where no sqlite3 binary is supplied (assumes python's sqlite3 module is compiled with rtree option)
    stmts = ["SELECT load_extension('mod_spatialite');"]
    for f in featlist:
        stmts.append("SELECT CreateSpatialIndex('{!s}', 'SHAPE');".format(f))
    feats = ', '.join(featlist)
    print('Creating spatial indices for:', feats)

    if not sqlite_bin:
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        print('Note: warning message of (updateTableTriggers: "no such module: rtree") means that the python sqlite3 module is not compiled with spatial index support. '
              'Spatial indices will not be created in this case.')
        for stmt in stmts:
            c.execute(stmt)
        conn.commit()
        conn.close()
    else:
        sql = ' '.join(stmts)
        p = subprocess.run([sqlite_bin, dbpath, sql], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        errors = p.stderr.decode('utf-8')
        if errors:
            print("Error:",errors)
        
    
if __name__ == "__main__":
    ### parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and import all found SSURGO data found '
                                     'in separate Soil Survey Area folders downloaded from NRCS into a SpatiaLite database.')
    ### positional arguments
    parser.add_argument('scanpath', help='path to recursively scan for SSURGO files')

    ### optional arguments
    dbpath_default = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'SSURGO.sqlite')
    parser.add_argument('-d', '--dbpath', metavar='"path/to/db.sqlite"', default=dbpath_default,
                        help='path to the SpatialLite database to import into, which will be created if does not exist')
    parser.add_argument('-e', '--ecosite', action='store_true',
                        help='creates a spatial feature called ecopolygon which shows the aggregate dominant ecological sites')
    parser.add_argument('-g', '--groups', metavar='"path/to/ecogroups.csv"',
                        help='imports ecogroups from comma delimited list and creates a spatial feature called ecogrouppolygon '
                        'which shows the aggregate dominant ecological groups')
    parser.add_argument('-i', '--index', nargs='?', default = None, const = '', metavar='"path/to/sqlite3"', 
                        help='adds spatial index to spatial features. Python sqlite module must be compiled with rtree '
                        'option to work natively. Otherwise an alternate sqlite binary can by supplied that does have rtree '
                        'compiled with "path/to/sqlite3" or a command in the PATH (e.g. sqlite3). WARNING: if an alternate binary '
                        'is supplied, subsequent imports into the same database may fail due to lack of native rtree function.')
    parser.add_argument('-r', '--repair', action='store_true',
                        help='attempts to repair any faulty geometry in original features and faulty geometries produced by the --snap option')
    parser.add_argument('-R', '--restrict', metavar='"path/to/list.csv"',
                        help='resticts imports to just those found in a comma delimited list (see list_example.csv)')
    parser.add_argument('-s', '--snap', default=0, type=float, metavar='grid_size_m',
                        help='the grid size (in meters) to snap features to')
    args = parser.parse_args()

    ### check for valid arguments
    if not os.path.isdir(args.scanpath):
        print("scanpath does not exist. Please choose an existing path to search.")
        quit()
    if not os.path.isdir(os.path.dirname(args.dbpath)):
        print("dbpath direcetory does not exist. Please choose an existing path to create db in.")
        quit()

    ### check for valid optional argumuments
    if args.groups is not None:
        if not os.path.isfile(args.groups):
            print(args.groups, "does not exist. Please choose an existing comma delimited ecogroup file to use.")
            quit()
    if args.restrict is not None:
        if not os.path.isfile(args.restrict):
            print(args.restrict, "does not exist. Please choose an existing comma delimited SSURGO list to use.")
            quit()

    if args.restrict is not None:
        with open(args.restrict, 'r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            survey_areas = []
            for row in csvreader:
                if row[0].strip() != '':
                    survey_areas.append(row[0].strip().lower())
    else:
        survey_areas = ''
    featlist = ['featline', 'featpoint','muline','mupoint','mupolygon','sapolygon']
    set_csv_limit()
    initdb(args.dbpath)
    new_imports = scan_insert(args.dbpath, args.scanpath, args.snap, survey_areas)

    if args.repair:
        repair_geom(args.dbpath, featlist)
    if args.ecosite:
        featlist.append('ecopolygon')
        if new_imports:
            make_eco(args.dbpath)
            if args.repair:
                repair_geom(args.dbpath, ['ecopolygon'])
    if args.groups is not None:
        featlist.append('ecogrouppolygon')
        create_ecogroups.create_table(args.dbpath)
        create_ecogroups.load_ecogroups(args.dbpath, args.groups)
        if new_imports:
            create_ecogroups.create_views(args.dbpath)
            if args.repair:
                repair_geom(args.dbpath, ['ecogrouppolygon'])
    if args.index is not None:
        create_spatial_indices(args.dbpath, args.index, featlist)
        
    print("Script finished.")
