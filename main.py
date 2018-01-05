#!/usr/bin/env python3
import os
import csv
import sys
import re
import sqlite3 as sqlite
from create_tables import table_statements
from create_views import view_statements
from create_custom import custom_statements

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

spatlist = [('soilmu_a', 'mupolygon', 'SHAPE, areasymbol, spatialver, musym, mukey', 'SELECT ST_Multi(ST_Union(Geometry)), AREASYMBOL, SPATIALVER, MUSYM, MUKEY FROM {!s} GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY'),
            ('soilmu_l', 'muline', 'SHAPE, areasymbol, spatialver, musym, mukey', 'SELECT ST_Multi(ST_Union(Geometry)), AREASYMBOL, SPATIALVER, MUSYM, MUKEY FROM {!s} GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY'),
            ('soilmu_p', 'mupoint', 'SHAPE, areasymbol, spatialver, musym, mukey', 'SELECT ST_Multi(ST_Union(Geometry)), AREASYMBOL, SPATIALVER, MUSYM, MUKEY FROM {!s} GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY'),
            ('soilsa_a', 'sapolygon', 'SHAPE, areasymbol, spatialver, lkey', 'SELECT ST_Multi(ST_Union(Geometry)), AREASYMBOL, SPATIALVER, LKEY FROM {!s} GROUP BY AREASYMBOL, SPATIALVER, LKEY'),
            ('soilsf_l', 'featline', 'SHAPE, areasymbol, spatialver, featsym, featkey', 'SELECT ST_Multi(ST_Union(Geometry)), AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY FROM {!s} GROUP BY AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY'),
            ('soilsf_p', 'featpoint', 'SHAPE, areasymbol, spatialver, featsym, featkey', 'SELECT ST_Multi(ST_Union(Geometry)), AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY FROM {!s} GROUP BY AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY')]

def printhelp():
    print("\nThis script will scan a folder and import all found SSURGO data found in separate Soil Survey Area "
          "folders downloaded from NRCS into a SpatiaLite database. These folders will contain (1) A folder for "
          "tabular data (pipe delimited format), (2) A folder for spatial data (shapefile format).")
    print("\nUsage syntax: import.py scanpath [dbpath]")
    print("\nscanpath is the path to recursively scan for SSURGO files and dbpath (optional) is the path to "
          "the SpatialLite database to import into. If dbpath is not given, a file named 'SSURGO.sqlite' will be "
          "created in the script directory.  If db path is given and does not exist, it will be created.\n")

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

def scan_insert(dbpath, scanpath):
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

                ### finds and inserts spstial data
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
                    print("Already imported ", current_ssa, ". Skipping ", os.path.join(root, f), sep = '')
                        
    c.execute("DELETE FROM virts_geometry_columns;")
    c.execute("DELETE FROM virts_geometry_columns_auth;")
    c.execute("DELETE FROM virts_geometry_columns_field_infos;")
    c.execute("DELETE FROM virts_geometry_columns_statistics;")
    conn.commit()
    conn.close()
    return new_imports

def make_custom(dbpath):
    print("Custom processing...")
    conn = sqlite.connect(dbpath)
    conn.enable_load_extension(True)
    c = conn.cursor()
    c.execute("SELECT load_extension('mod_spatialite');")
    for stmt in custom_statements:
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
            if len(sys.argv) > 2:
                dbpath = str(sys.argv[2])
            else:
                dbpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'SSURGO.sqlite')
            scanpath = os.path.abspath(str(sys.argv[1]))
    else:
        printhelp()
        quit()
    if not os.path.isdir(scanpath):
        print("scanpath does not exist. Please choose an existing path to search.")
        quit()

    set_csv_limit()
    initdb(dbpath)
    new_imports = scan_insert(dbpath, scanpath)
    if new_imports:
        make_custom(dbpath)
    print("Script finished.")
