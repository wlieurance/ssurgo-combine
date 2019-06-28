#!/usr/bin/env python3
import os
import csv
import sys
import re
import sqlite3 as sqlite
import psycopg2
import pyodbc
import argparse
import subprocess
import shapefile
import pygeoif
import datetime
from create_tables import table_statements, temp_tables, spatialite_addgeom, postgis_addgeom, \
                          spatialite_temp_addgeom, postgis_temp_addgeom   # , postgis_addgeom, mssql_addgeom
from create_views import spatialite_views
from create_views import postgis_views
from create_custom import custom_spatialite, custom_postgis
import create_ecogroups
import delete
# from create_ecogroups import ecogroup_spatialite, ecogroup_postgis
from config import spatialite_tables, postgis_tables, mssql_tables


def get_default_schema(dbtype, schema):
    if dbtype == 'spatialite':
        schema = 'main'
    if not schema:
        if dbtype == 'postgis':
            schema = 'public'
        elif dbtype == 'mssql':
            schema = 'dbo'
    return schema


def initdb(dbpath, schema, dbtype):
    """connect to database, initialize if necessary and create tables."""
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
        c.execute("SELECT Count(*) FROM sqlite_master WHERE type = 'table' "
                  "AND name NOT LIKE 'sqlite_%' and name = 'mupolygon';")
        objects = c.fetchone()[0]
        if objects >= 1:
            newdb = False
        else:
            newdb = True
        if newdb:
            print("Creating tables...")
            c.execute("SELECT InitSpatialMetaData(1);")
            spatialite_tables['schema'] = schema
            for stmt in table_statements + spatialite_addgeom:
                s = stmt.replace(" REFERENCES {schema}.", " REFERENCES ")  # sqlite doesn't like schema in REFERENCES
                # print(s.format(**spatialite_tables))
                c.execute(s.format(**spatialite_tables))
            conn.commit()
            print("Creating views...")
            for stmt in spatialite_views:
                c.execute(stmt.format(**spatialite_tables))
            conn.commit()
        conn.close()

    # connect to postgres database, initialize if necessary and create tables.
    if dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        c.execute("SELECT Count(*) FROM information_schema.tables WHERE table_name = 'mupolygon';")
        objects = c.fetchone()[0]
        if objects >= 1:
            newdb = False
        else:
            newdb = True
        postgis_tables['schema'] = schema
        if newdb:
            print("Creating tables...")
            c.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            c.execute("CREATE SCHEMA IF NOT EXISTS {!s};".format(schema))
            for stmt in table_statements + postgis_addgeom:
                c.execute(stmt.format(**postgis_tables))
            conn.commit()
            print("Creating views...")
            for stmt in postgis_views:
                c.execute(stmt.format(**postgis_tables))
            conn.commit()
        conn.close()

    # connect to postgres database, initialize if necessary and create tables.
    if dbtype == 'mssql':
        dbpath = dbpath + ';driver={ODBC Driver 13 for SQL Server}'
        conn = pyodbc.connect(dbpath)
        c = conn.cursor()
        c.execute("SELECT Count(*) FROM information_schema.tables WHERE table_schema = 'dbo';")
        objects = c.fetchone()[0]
        if objects >= 90:
            newdb = False
        else:
            newdb = True
        if newdb:
            mssql_tables['schema'] = schema
            c.execute("IF NOT EXISTS (SELECT  schema_name FROM  information_schema.schemata"
                      " WHERE  schema_name = '{schema}' )"
                      " BEGIN EXEC sp_executesql N'CREATE SCHEMA {schema}' END GO".format(**{'schema': schema}))
            print("Creating tables...")
            for stmt in table_statements:
                print(stmt.format(**mssql_tables))
                c.execute(stmt.format(**mssql_tables))
            conn.commit()
            print("Creating views...")
            for stmt in postgis_views:
                c.execute(stmt.format(**mssql_tables))
            conn.commit()
        conn.close()


def set_csv_limit():
    """sets the the field size limit to as large as possible in order to deal with the legendtext entries, which are
    large ungainly strings"""
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


def scan_insert(dbpath, schema, dbtype, scanpath, snap, repair, skip, survey_areas):
    """scans the scanpath and inserts new tabular and spatial data from soil areas that do not exist into the DB."""
    # define variables
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
               ('sdvfolderattribute.txt', 'sdvfolderattribute')]
    if skip:
        skiplist = skip.split(',')
        for s in skiplist:
            for i in tablist:
                if s == i[1]:
                    tablist.remove(i)

    if snap == 0:
        if repair:
            geom = "ST_Multi(ST_Union(ST_MakeValid(geom)))"
            print("Importing: no snapping with repair...")
        else:
            geom = "ST_Multi(ST_Union(geom))"
            print("Importing: no snapping no repair...")
    else:
        # '{:.20f}'.format(snap/111319.9) converts meters to decimal degrees and formats for non-scientific notation
        if repair:
            geom = "ST_SnapToGrid(ST_Multi(ST_Union(ST_MakeValid(geom))), {!s})"\
                .format('{:.20f}'.format(snap/111319.9))
            print("Importing with snapping with repair...")
        else:
            geom = "ST_SnapToGrid(ST_Multi(ST_Union(geom)), {!s})".format('{:.20f}'.format(snap/111319.9))
            print("Importing with snapping no repair...")
        
    soilmu_a_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY").format(geom)

    soilmu_l_sql = ("SELECT {!s}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {{!s}}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY").format(geom)

    # .format(geom.replace('ST_Multi','').replace('ST_Union','')) for POINT vs MULTIPOINT (remove GROUP BY clause)
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

    spatlist = [('soilmu_a', 'mupolygon', 'geom, areasymbol, spatialver, musym, mukey', soilmu_a_sql),
                ('soilmu_l', 'muline', 'geom, areasymbol, spatialver, musym, mukey', soilmu_l_sql),
                ('soilmu_p', 'mupoint', 'geom, areasymbol, spatialver, musym, mukey', soilmu_p_sql),
                ('soilsa_a', 'sapolygon', 'geom, areasymbol, spatialver, lkey', soilsa_a_sql),
                ('soilsf_l', 'featline', 'geom, areasymbol, spatialver, featsym, featkey', soilsf_l_sql),
                ('soilsf_p', 'featpoint', 'geom, areasymbol, spatialver, featsym, featkey', soilsf_p_sql)]
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
        spatialite_tables['schema'] = schema
        for stmt in temp_tables + spatialite_temp_addgeom:
            # print(stmt)
            c.execute(stmt.format(**spatialite_tables))
        conn.commit()
        paramstr = '?'
        ins1 = 'OR IGNORE'
        ins2 = ''
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        postgis_tables['schema'] = schema
        for stmt in temp_tables + postgis_temp_addgeom:
            c.execute(stmt.format(**postgis_tables))
        conn.commit()
        paramstr = '%s'
        ins1 = ''
        ins2 = 'ON CONFLICT DO NOTHING'
    existing = []
    c.execute("SELECT areasymbol, saversion FROM {schema}.sacatalog ORDER BY areasymbol;"
              .format(**{'schema': schema}))
    for row in c:
        existing.append({'ssa': row[0].lower(), 'version': row[1]})
    current_ssa = ''
    new_imports = False
    # searches scanpath for SSURGO tabular data and imports into dbpath
    for root, dirs, files in os.walk(scanpath):
        for f in files:
            if f.find('soil_metadata_') >= 0 and f.find('.txt') > 0:
                new_ssa = True
                current_ssa = re.findall(r'soil_metadata_(.*?).txt', f)[0]
                with open(os.path.join(root, 'tabular', 'sacatlog.txt'), 'r') as textfile:
                    reader = csv.reader(textfile, quotechar='"', delimiter='|')
                    scan_ssa_version = int(next(reader)[2])
                print("Current SSA is", current_ssa, "version", scan_ssa_version)
                ssa_deleted = False
                if current_ssa.lower() in survey_areas or survey_areas == '':
                    if current_ssa.lower() in [x['ssa'] for x in existing]:
                        existing_ssa_version = [x['version'] for x in existing if x['ssa'] == current_ssa.lower()][0]
                        if existing_ssa_version < scan_ssa_version:
                            print("Replacing", current_ssa.lower(), "version", existing_ssa_version,
                                  "with version", scan_ssa_version)
                            delete.delete_ssa(dbpath, schema, dbtype, current_ssa.lower())
                            ssa_deleted = True
            if current_ssa.lower() in survey_areas or survey_areas == '':
                if current_ssa.lower() not in [x['ssa'] for x in existing] or ssa_deleted:
                    # finds and inserts tabular data
                    if f in [(i[0]) for i in tablist]:
                        tbl = [t[1] for t in tablist if t[0] == f][0]
                        print("Importing table: ", current_ssa + ' | ' + tbl, sep='')
                        new_imports = True
                        c.execute("SELECT * FROM {table} LIMIT 1;".format(**{'table': '.'.join((schema, tbl))}))
                        fields = [description[0] for description in c.description]
                        SQL = "INSERT {{!s}} INTO {!s} VALUES ({!s}) {{!s}};"\
                            .format('.'.join((schema, tbl)), ','.join([paramstr]*len(fields)))
                        SQL = SQL.format(ins1, ins2)
                        if dbtype == 'postgis':
                            c.execute("ALTER TABLE {table} DISABLE TRIGGER ALL;"
                                      .format(**{'table': '.'.join((schema, tbl))}))
                        with open(os.path.join(root, f), 'r') as textfile:
                            dictread = csv.DictReader(textfile, fieldnames=fields, quotechar='"', delimiter='|')
                            for row in dictread:
                                for key, value in row.items():
                                    if value == '':
                                        row[key] = None
                                # print(current_ssa, tbl, row.values(), sep = ' | ')
                                c.execute(SQL, list(row.values()))
                        if dbtype == 'postgis':
                            c.execute("ALTER TABLE {table} ENABLE TRIGGER ALL;"
                                      .format(**{'table': '.'.join((schema, tbl))}))
                        conn.commit()

                    # finds and inserts spatial data
                    elif any(s in f for s in [i[0] for i in spatlist]) and os.path.splitext(f)[1] == '.shp':
                        # picks out the appropriate item from the spatial list (spatlist)
                        index = [s in f for s in [i[0] for i in spatlist]].index(True)
                        sublist = spatlist[index]  # subsets spatlist to the appropriate item

                        # list for converting shapefile.Reader() items to well known text (shapefile.Reader() int,
                        # shapefile.Reader() text, Well Known Text)
                        insert_table = '.'.join((schema, sublist[1]))
                        insert_text = sublist[2]
                        select_stmt = sublist[3]
                        shp_table = '_'.join((insert_table, 'shp'))
                        print("Importing shapefile: ", current_ssa + ' | ' + os.path.splitext(f)[0], sep='')
                        new_imports = True
                        iSQL = "INSERT INTO {!s} ({!s}) VALUES ({!s},{!s})"\
                            .format(shp_table, insert_text, '{!s}', ','.join((len(insert_text.split())-1) * [paramstr]))
                        sf = shapefile.Reader(os.path.join(root, f))
                        shrecs = sf.shapeRecords()
                        for rec in shrecs:
                            wtk = ''.join(("ST_GeomFromText('", str(pygeoif.geometry.as_shape(rec.shape)), "', 4326)"))
                            sql = iSQL.format(wtk)
                            c.execute(sql, rec.record)
                        gSQL = "INSERT INTO {!s} ({!s}) {!s};".format(insert_table, insert_text,
                                                                      select_stmt.format(shp_table))
                        # print(gSQL)
                        if dbtype == 'postgis':
                            c.execute("ALTER TABLE {table} DISABLE TRIGGER ALL;"
                                      .format(**{'table': insert_table}))
                        c.execute(gSQL)
                        if dbtype == 'postgis':
                            c.execute("ALTER TABLE {table} ENABLE TRIGGER ALL;"
                                      .format(**{'table': insert_table}))
                        c.execute("DELETE FROM {!s};".format(shp_table))
                        conn.commit()
                else:
                    if current_ssa != '' and new_ssa:
                        print('Already imported ', current_ssa, '. Skipping...', sep='')
                        new_ssa = False
                    
    # delete vestige virtual table information from shapefile import
    shp_list = ['_'.join((i[1], 'shp')) for i in spatlist]
    for i in shp_list:
        if dbtype == 'spatialite':
            sql = "SELECT DiscardGeometryColumn('{!s}', 'geom');".format(i)
            c.execute(sql)
        c.execute("DROP TABLE {schema}.{table};".format(**{'schema': schema, 'table': i}))
    conn.commit()
    conn.close()
    return new_imports


def make_eco(dbpath, schema, dbtype):
    """Creates custom spatial ecosite features."""
    print("Creating ecopolygon feature...")
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
        spatialite_tables['schema'] = schema
        for stmt in custom_spatialite:
            c.execute(stmt.format(**spatialite_tables))
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        postgis_tables['schema'] = schema
        for stmt in custom_postgis:
            c.execute(stmt.format(**postgis_tables))
    conn.commit()
    conn.close()


def repair_geom(dbpath, schema, dbtype, featlist):
    """repairs geometries of final spatial features if that option was selected"""
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        
    # each entry in tables is: tablename, SELECT string, geometry type for ST_CollectionExtract()
    tables = [('mupolygon', 'areasymbol, spatialver, musym, mukey, area_ha', 3),
              ('muline', 'areasymbol, spatialver, musym, mukey, length_m', 2),
              ('mupoint', 'areasymbol, spatialver, musym, mukey, x, y', 1),
              ('sapolygon', 'areasymbol, spatialver, lkey, area_ha', 3),
              ('featline', 'areasymbol, spatialver, featsym, featkey, length_m', 2),
              ('featpoint', 'areasymbol, spatialver, featsym, featkey, x, y', 1),
              ('ecopolygon', 'ecoclassid_std, ecoclassname, ecotype, area_ha, ecopct', 3),
              ('ecogrouppolygon', 'ecogroup, groupname, grouptype, pub_status, area_ha, ecogrouppct', 3)]
    use_tables = [k for k in tables if k[0] in featlist]  # restricts tables to only features that exist (featlist)
    
    for t in use_tables:
        tbl = '.'.join((schema, t[0]))
        print("Repairing {!s} geometries...".format(t[0]))
        temp_sql = ("CREATE TEMP TABLE {!s}_temp AS SELECT {!s}, ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), "
                    "{!s})) AS geom FROM {!s};".format(t[0], t[1], t[2], tbl))
        # relic for converting MULTIPOINT to POINT. decided to use multipoint for consistency
        # if 'point' in t[0]:
        #     temp_sql = temp_sql.replace('ST_Multi', '')
        c.execute(temp_sql)
        c.execute("DELETE FROM {!s};".format(tbl))
        iSQL = "INSERT INTO {!s} ({!s}) SELECT {!s} FROM {!s}_temp;"\
            .format(tbl, ', '.join((t[1], 'geom')), ', '.join((t[1], 'geom')), t[0])
        c.execute(iSQL)
    conn.commit()
    conn.close()


def create_spatial_indices(dbpath, schema, dbtype, sqlite_bin, featlist):
    """adds R*Tree spatial indices. if dbtype is spatialite, needs the python sqlite3 module to be compiled with the
    rtree option.  A sqlite binary with R*tree present can be passed as an option, but will break future imports."""
    feats = ', '.join(featlist)
    print('Creating spatial indices for:', feats)
    if dbtype == 'spatialite':
        stmts = ["SELECT load_extension('mod_spatialite');"]
        for f in featlist:
            stmts.append("SELECT CreateSpatialIndex('{!s}', 'geom');".format(f))
        if sqlite_bin == 'no arg':
            conn = sqlite.connect(dbpath)
            conn.enable_load_extension(True)
            c = conn.cursor()
            print('Note: warning message of (updateTableTriggers: "no such module: rtree") means that the python '
                  'sqlite3 module is not compiled with spatial index support. Spatial indices will not be created in '
                  'this case.')
            for stmt in stmts:
                success = c.execute(stmt).fetchone()[0]
            for f in featlist:
                valid = c.execute("SELECT CheckSpatialIndex('{!s}', 'geom');".format(f)).fetchone()[0]
                if valid is None:
                    c.execute("SELECT DisableSpatialIndex('{!s}', 'geom');".format(f)).fetchone()[0]
                    print("disabling index due top lack of Rtree for", f)

            conn.commit()
            conn.close()
        else:
            sql = ' '.join(stmts)
            p = subprocess.run([sqlite_bin, dbpath, sql], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            errors = p.stderr.decode('utf-8')
            if errors:
                print("Error:", errors)
    elif dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        for f in featlist:
            c.execute("DROP INDEX IF EXISTS {schema}.{name}_gix;".format(**{'schema': schema, 'name': f}))
            c.execute("CREATE INDEX {name}_gix ON {schema}.{table} USING GIST (geom);"
                      .format(**{'schema': schema, 'name': f, 'table': f}))
            c.execute("ANALYZE {schema}.{table};".format(**{'schema': schema, 'table': f}))
        conn.commit()
        conn.close()


def add_area(dbpath, schema, dbtype, featlist, force):
    """Adds geometry measurements to final features"""
    if not force:
        sql_up = "UPDATE {!s} SET {!s} = {!s} WHERE {!s} IS NULL;"
    else:
        sql_up = "UPDATE {!s} SET {!s} = {!s} /* WHERE {!s} IS NULL */;"
    
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")

        for f in featlist:
            xf = '.'.join((schema, f))
            print("Adding geometry measurements to " + f)
            if 'poly' in f:
                c.execute(sql_up.format(xf, 'area_ha', 'st_area(geom, 1)/10000', 'area_ha'))
            elif 'line' in f:
                c.execute(sql_up.format(xf, 'length_m', 'st_length(geom, 1)', 'length_m'))
            elif 'point' in f:
                c.execute(sql_up.format(xf, 'x', 'st_x(st_centroid(geom))', 'x'))
                c.execute(sql_up.format(xf, 'y', 'st_y(st_centroid(geom))', 'y'))
        conn.commit()
        conn.close()
    if dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
        for f in featlist:
            xf = '.'.join((schema, f))
            print("Adding geometry measurements to " + f)
            if 'poly' in f:
                c.execute(sql_up.format(xf, 'area_ha', 'st_area(geography(geom))/10000', 'area_ha'))
            elif 'line' in f:
                c.execute(sql_up.format(xf, 'length_m', 'st_length(geography(geom))', 'length_m'))
            elif 'point' in f:
                c.execute(sql_up.format(xf, 'x', 'st_x(st_centroid(geom))', 'x'))
                c.execute(sql_up.format(xf, 'y', 'st_y(st_centroid(geom))', 'y'))
        conn.commit()
        conn.close()
        
    
if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and import all found SSURGO data '
                                                 'found in separate Soil Survey Area folders downloaded from NRCS into '
                                                 'a SpatiaLite database.')
    # positional arguments
    parser.add_argument('scanpath', help='path to recursively scan for SSURGO files')

    # optional arguments
    dbpath_default = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'SSURGO.sqlite')
    parser.add_argument('-d', '--dbpath', metavar='CONNECTION_STRING', default=dbpath_default,
                        help='if db type is spatialite, then this is the path to the SpatialLite database to import '
                             'into, (e.g. ("path/to/db.sqlite") which will be created if does not exist. Otherwise, '
                             'this is the database connection string for your database. (e.g. "dbname=somedb '
                             'user=someuser password=\'somepass\'" for postgis')
    # or "server=SOME\SQLSERVER;database=somedb;trusted_connection=yes" for mssql)
    parser.add_argument('-e', '--ecosite', action='store_true',
                        help='creates a spatial feature called ecopolygon which shows the aggregate dominant '
                             'ecological sites')
    parser.add_argument('-g', '--groups', metavar='"path/to/ecogroup_meta.csv","/path/to/ecogroup.csv"',
                        help='paths of the csv files containing the ecological group metadata and the sites '
                             'within each ecogroup (see ecogroups_example.csv and ecogroups_meta_example.csv)')
    parser.add_argument('-i', '--index', nargs='?', default=None, const='no arg', metavar='"path/to/sqlite3"',
                        help='adds spatial index to spatial features. Python sqlite module must be compiled with rtree '
                             'option to work natively. Otherwise an alternate sqlite binary can by supplied that does '
                             'have rtree compiled with "path/to/sqlite3" or a command in the PATH (e.g. sqlite3). '
                             'WARNING: if an alternate binary is supplied, subsequent imports into the same database '
                             'may fail due to lack of native rtree function.')
    parser.add_argument('-r', '--repair', action='store_true',
                        help='attempts to repair any faulty geometry in original features and faulty geometries '
                             'produced by the --snap option')
    parser.add_argument('-R', '--restrict', metavar='"path/to/list.csv"',
                        help='restricts imports to just those found in a comma delimited list (see list_example.csv)')
    parser.add_argument('-s', '--snap', default=0, type=float, metavar='grid_size_m',
                        help='the grid size (in meters) to snap features to')
    parser.add_argument('-t', '--type', metavar='DATABASE_TYPE', default='spatialite',
                        help='decides which database type to import to: spatialite, postgis') #, or mssql
    parser.add_argument('-k', '--skip', metavar='tables,to,skip',
                        help='a comma separated list of tabular tables to skip during the import (e.g. cointerp). '
                             'WARNING: skipping tables can be dangerous if they are referenced by a FOREIGN KEY '
                             'CONSTRAINT. Use caution.')
    parser.add_argument('-f', '--force', action='store_true',
                        help='will force the recreation/repair of ecosite and ecogroup layers even if no new imports '
                             'are found')
    parser.add_argument('-x', '--schema', default='',
                        help='tells the script what schema to use (do not use for spatialite, or use "main" '
                             'for schema name)')
    args = parser.parse_args()

    # check for valid arguments
    if not os.path.isdir(args.scanpath):
        print("scanpath does not exist. Please choose an existing path to search.")
        quit()
    if args.type == 'spatialite':
        if not os.path.isdir(os.path.dirname(args.dbpath)):
            print("dbpath directory does not exist. Please choose an existing path to create db in.")
            quit()

    # check for valid optional arguments
    if args.groups is not None:
        good_csv_paths = True
        csvmetapath = args.groups.split(',')[0].strip().replace('"', '')
        csvpath = args.groups.split(',')[1].strip().replace('"', '')
        if not os.path.isfile(csvmetapath):
            print(csvmetapath + " does not exist. Please choose an existing comma delimited ecogroup_meta file to use.")
            good_csv_paths = False
            quit()
        if not os.path.isfile(csvpath):
            print(csvpath + " does not exist. Please choose an existing comma delimited ecogroup file to use.")
            good_csv_paths = False
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
    featlist = ['featline', 'featpoint', 'muline', 'mupoint', 'mupolygon', 'sapolygon']
    set_csv_limit()
    schema = get_default_schema(args.type, args.schema)
    initdb(args.dbpath, schema, args.type)
    new_imports = scan_insert(args.dbpath, schema, args.type, args.scanpath, args.snap, args.repair, args.skip,
                              survey_areas)

    if (args.repair and new_imports) or (args.repair and args.force):
        repair_geom(args.dbpath, schema, args.type, featlist)

    if new_imports or args.force:
        add_area(args.dbpath, schema, args.type, featlist, args.force)
            
    if args.ecosite:
        featlist.append('ecopolygon')
        if new_imports or args.force:
            make_eco(args.dbpath, schema, args.type)
            if args.repair:
                repair_geom(args.dbpath, schema, args.type, ['ecopolygon'])
                add_area(args.dbpath, schema, args.type, ['ecopolygon'], True)
    if args.groups is not None:
        if good_csv_paths:
            featlist.append('ecogrouppolygon')
            create_ecogroups.create_table(args.dbpath, schema, args.type)
            create_ecogroups.load_ecogroups(args.dbpath, schema, args.type, csvmetapath, csvpath)
            if new_imports or args.force:
                create_ecogroups.create_views(args.dbpath, schema, args.type)
                if args.repair:
                    repair_geom(args.dbpath, schema, args.type, ['ecogrouppolygon'])
                    add_area(args.dbpath, schema, args.type, ['ecogrouppolygon'], True)
    if args.index is not None:
        create_spatial_indices(args.dbpath, schema, args.type, args.index, featlist)
        
    print("Script finished.")
