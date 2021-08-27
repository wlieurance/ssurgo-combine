#!/usr/bin/env python3
import csv
import sys
import sqlite3 as sqlite
import psycopg2
import pyodbc
import argparse
import shapefile
import pygeoif
import getpass

# custom modules
import create_ecogroups
import delete
from config import spatialite_regex, mssql_regex
from parse_sql import *

tabular_list = [
    ('msdommas.txt', 'mdstatdommas', 1),
    ('mstab.txt', 'mdstattabs', 2),
    # (None, 'month', 3),
    ('sacatlog.txt', 'sacatalog', 4),
    ('sdvalgorithm.txt', 'sdvalgorithm', 5),
    ('sdvattribute.txt', 'sdvattribute', 6),
    ('sdvfolder.txt', 'sdvfolder', 7),
    ('soilsf_t_', 'featdesc', 8),
    ('legend.txt', 'legend', 9),
    ('msdomdet.txt', 'mdstatdomdet', 10),
    ('msidxmas.txt', 'mdstatidxmas', 11),
    ('msrsmas.txt', 'mdstatrshipmas', 12),
    ('mstabcol.txt', 'mdstattabcols', 13),
    ('sdvfolderattribute.txt', 'sdvfolderattribute', 14),
    ('distlmd.txt', 'distlegendmd', 15),
    ('distmd.txt', 'distmd', 16),
    ('distimd.txt', 'distinterpmd', 17),
    ('lareao.txt', 'laoverlap', 18),
    ('ltext.txt', 'legendtext', 19),
    ('mapunit.txt', 'mapunit', 20),
    ('msidxdet.txt', 'mdstatidxdet', 21),
    ('msrsdet.txt', 'mdstatrshipdet', 22),
    ('sainterp.txt', 'sainterp', 23),
    ('comp.txt', 'component', 24),
    ('muaggatt.txt', 'muaggatt', 25),
    ('muareao.txt', 'muaoverlap', 26),
    ('mucrpyd.txt', 'mucropyld', 27),
    ('mutext.txt', 'mutext', 28),
    ('chorizon.txt', 'chorizon', 29),
    ('ccancov.txt', 'cocanopycover', 30),
    ('ccrpyd.txt', 'cocropyld', 31),
    ('cdfeat.txt', 'codiagfeatures', 32),
    ('cecoclas.txt', 'coecoclass', 33),
    ('ceplants.txt', 'coeplants', 34),
    ('cerosnac.txt', 'coerosionacc', 35),
    ('cfprod.txt', 'coforprod', 36),
    ('cgeomord.txt', 'cogeomordesc', 37),
    ('chydcrit.txt', 'cohydriccriteria', 38),
    ('cinterp.txt', 'cointerp', 39),
    ('cmonth.txt', 'comonth', 40),
    ('cpmatgrp.txt', 'copmgrp', 41),
    ('cpwndbrk.txt', 'copwindbreak', 42),
    ('crstrcts.txt', 'corestrictions', 43),
    ('csfrags.txt', 'cosurffrags', 44),
    ('ctxfmmin.txt', 'cotaxfmmin', 45),
    ('ctxmoicl.txt', 'cotaxmoistcl', 46),
    ('ctext.txt', 'cotext', 47),
    ('ctreestm.txt', 'cotreestomng', 48),
    ('ctxfmoth.txt', 'cotxfmother', 49),
    ('chaashto.txt', 'chaashto', 50),
    ('chconsis.txt', 'chconsistence', 51),
    ('chdsuffx.txt', 'chdesgnsuffix', 52),
    ('chfrags.txt', 'chfrags', 53),
    ('chpores.txt', 'chpores', 54),
    ('chstrgrp.txt', 'chstructgrp', 55),
    ('chtext.txt', 'chtext', 56),
    ('chtexgrp.txt', 'chtexturegrp', 57),
    ('chunifie.txt', 'chunified', 58),
    ('cfprodo.txt', 'coforprodo', 59),
    ('cpmat.txt', 'copm', 60),
    ('csmoist.txt', 'cosoilmoist', 61),
    ('cstemp.txt', 'cosoiltemp', 62),
    ('csmorgc.txt', 'cosurfmorphgc', 63),
    ('csmorhpp.txt', 'cosurfmorphhpp', 64),
    ('csmormr.txt', 'cosurfmorphmr', 65),
    ('csmorss.txt', 'cosurfmorphss', 66),
    ('chstr.txt', 'chstruct', 67),
    ('chtextur.txt', 'chtexture', 68),
    ('chtexmod.txt', 'chtexturemod', 69),
]


feature_list = ['featline', 'featpoint', 'muline', 'mupoint', 'mupolygon', 'sapolygon']


class DbConnect:
    def __init__(self, dbtype, dbname, user=None, port=None, instance=None, host='localhost', pwd=None, schema=None):
        self.dbtype = dbtype
        self.user = user
        self.dbname = dbname
        self.port = port
        self.instance = instance
        self.host = host
        self.pwd = pwd
        self.schema = schema
        self.tc = None
        self.server = None
        self.conn = None
        self.connected = False

    def open(self):
        if self.dbtype == 'spatialite':
            self.conn = sqlite.connect(self.dbname)
            self.conn.enable_load_extension(True)
            self.conn.execute("SELECT load_extension('mod_spatialite');")
            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.connected = True
        elif self.dbtype == 'postgis':
            if self.pwd is None:
                self.pwd = getpass.getpass()
            if self.user is None:
                self.user = 'postgres'
            if self.port is None:
                self.port = 5432
            if self.dbname is None:
                self.dbname = 'postgres'
            self.conn = psycopg2.connect(user=self.user, host=self.host, dbname=self.dbname, port=self.port,
                                         password=self.pwd)
            self.connected = True
            if self.schema:
                self.conn.execute(f"SET search_path = {self.schema};")

        elif self.dbtype == 'mssql':
            if self.user is None:
                self.tc = 'yes'
            else:
                self.tc = 'no'
                if self.pwd is None:
                    self.pwd = getpass.getpass()
            if self.dbname is None:
                self.dbname = 'master'
            self.server = ','.join([str(y) for y in ('\\'.join([x for x in (self.host, self.instance) if x]), self.port)
                                    if y])
            self.conn = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=self.server,
                                       database=self.dbname, trusted_connection=self.tc, uid=self.user, pwd=self.pwd)
            self.connected = True

    def close(self):
        if self.conn:
            self.conn.close()
            self.connected = False


def get_default_schema(dbtype, schema):
    if dbtype == 'spatialite':
        schema = 'main'
    if not schema:
        if dbtype == 'postgis':
            schema = 'public'
        elif dbtype == 'mssql':
            schema = 'dbo'
    return schema


def execute_statements(db, stmts, schema, params={}):
    db.open()
    c = db.conn.cursor()
    statements = stmts.copy()
    if db.dbtype == 'spatialite':
        regex_list = spatialite_regex
    elif db.dbtype == 'mssql':
        regex_list = mssql_regex
    else:
        regex_list = None
    if regex_list:
        for r in regex_list:
            pattern = re.compile(r['pattern'], re.I)
            statements = [re.sub(pattern, r['repl'], x) for x in statements]
    params['schema'] = schema
    formatted_stmts = [x.format(**params) for x in statements]
    for stmt in formatted_stmts:
        try:
            c.execute(stmt)
        except (sqlite.Error, pyodbc.Error, psycopg2.Error):
            print(stmt)
            raise
        else:
            db.conn.commit()
    db.close()


def initdb(db, schema):
    """connect to database, initialize if necessary and create tables."""
    db.open()
    c = db.conn.cursor()
    newdb = True
    ecopoly = False
    grppoly = False

    if db.dbtype == 'spatialite':
        c.execute("SELECT Count(*) FROM sqlite_master WHERE type = 'table' "
                  "AND name NOT LIKE 'sqlite_%' and name = 'spatial_ref_sys';")
        objects = c.fetchone()[0]
        if objects == 0:
            c.execute("SELECT InitSpatialMetaData(1);")
        c.execute("SELECT Count(*) FROM sqlite_master WHERE type = 'table' "
                  "AND name NOT LIKE 'sqlite_%' and name = 'mupolygon';")
        objects = c.fetchone()[0]
        if objects >= 1:
            newdb = False
        c.execute("SELECT Count(*) FROM sqlite_master WHERE type = 'table' "
                  "AND name NOT LIKE 'sqlite_%' and name = 'ecopolygon';")
        objects = c.fetchone()[0]
        if objects >= 1:
            ecopoly = True
        c.execute("SELECT Count(*) FROM sqlite_master WHERE type = 'table' "
                  "AND name NOT LIKE 'sqlite_%' and name = 'ecogrouppolygon';")
        objects = c.fetchone()[0]
        if objects >= 1:
            grppoly = True

    # connect to postgres database, initialize if necessary and create tables.
    if db.dbtype == 'postgis':
        c.execute("SELECT Count(*) FROM information_schema.tables WHERE table_name = 'mupolygon' "
                  "AND table_schema = %s;", (schema,))
        objects = c.fetchone()[0]
        if objects >= 1:
            newdb = False
        else:
            c.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            c.execute("CREATE SCHEMA IF NOT EXISTS {!s};".format(schema))

        c.execute("SELECT Count(*) FROM information_schema.tables WHERE table_name = 'ecopolygon' "
                  "AND table_schema = %s;", (schema,))
        objects = c.fetchone()[0]
        if objects >= 1:
            ecopoly = True
        c.execute("SELECT Count(*) FROM information_schema.tables WHERE table_name = 'ecogrouppolygon' "
                  "AND table_schema = %s;", (schema,))
        objects = c.fetchone()[0]
        if objects >= 1:
            grppoly = True

    # connect to postgres database, initialize if necessary and create tables.
    if db.dbtype == 'mssql':
        c.execute("SELECT Count(*) FROM information_schema.tables WHERE table_schema = 'dbo';")
        objects = c.fetchone()[0]
        if objects >= 90:
            newdb = False
        else:
            c.execute("IF NOT EXISTS (SELECT  schema_name FROM  information_schema.schemata"
                      " WHERE  schema_name = '{schema}' )"
                      " BEGIN EXEC sp_executesql N'CREATE SCHEMA {schema}' END GO".format(**{'schema': schema}))
            db.conn.commit()
        # insert code here for checking ecopolygon and ecogrouppolygon status
    db.conn.commit()
    db.close()
    t_parser = SqlParser(file=r'sql/create_tables.sql')
    v_parser = SqlParser(file=r'sql/create_views.sql')
    t_statements = t_parser.sql
    v_statements = v_parser.sql
    print('Creating tables...')
    execute_statements(stmts=t_statements, schema=schema, db=db)
    print('Creating views...')
    execute_statements(stmts=v_statements, schema=schema, db=db)
    return newdb, ecopoly, grppoly


def set_csv_limit():
    """sets the the field size limit to as large as possible in order to deal with the legendtext entries, which are
    large ungainly strings"""
    maxint = sys.maxsize
    decrement = True
    while decrement:
        # decrease the maxInt value by factor 10 
        # as long as the OverflowError occurs.
        decrement = False
        try:
            csv.field_size_limit(maxint)
        except OverflowError:
            maxint = int(maxint/10)
            decrement = True


def scan_insert(db, schema, scanpath, snap, repair, skip, survey_areas, ignore=True):
    """scans the scanpath and inserts new tabular and spatial data from soil areas that do not exist into the DB."""
    # define variables
    si_support = True
    if skip:
        for s in skip:
            for i in tabular_list:
                if s == i[1]:
                    tabular_list.remove(i)

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
        
    soilmu_a_sql = (f"SELECT {geom}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {schema}.{tbl}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY")

    soilmu_l_sql = (f"SELECT {geom}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {schema}.{tbl}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY")

    # .format(geom.replace('ST_Multi','').replace('ST_Union','')) for POINT vs MULTIPOINT (remove GROUP BY clause)
    soilmu_p_sql = (f"SELECT {geom}, AREASYMBOL, SPATIALVER, MUSYM, MUKEY"
                    "  FROM {schema}.{tbl}"
                    " GROUP BY AREASYMBOL, SPATIALVER, MUSYM, MUKEY")

    soilsa_a_sql = (f"SELECT {geom}, AREASYMBOL, SPATIALVER, LKEY"
                    "  FROM {schema}.{tbl}"
                    " GROUP BY AREASYMBOL, SPATIALVER, LKEY")

    soilsf_l_sql = (f"SELECT {geom}, AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY"
                    "  FROM {schema}.{tbl}"
                    " GROUP BY AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY")

    soilsf_p_sql = (f"SELECT {geom}, AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY"
                    "  FROM {schema}.{tbl}"
                    " GROUP BY AREASYMBOL, SPATIALVER, FEATSYM, FEATKEY")

    spatial_list = [
        ('soilsa_a', 'sapolygon', 'geom, areasymbol, spatialver, lkey', soilsa_a_sql),
        ('soilsf_l', 'featline', 'geom, areasymbol, spatialver, featsym, featkey', soilsf_l_sql),
        ('soilsf_p', 'featpoint', 'geom, areasymbol, spatialver, featsym, featkey', soilsf_p_sql),
        ('soilmu_l', 'muline', 'geom, areasymbol, spatialver, musym, mukey', soilmu_l_sql),
        ('soilmu_p', 'mupoint', 'geom, areasymbol, spatialver, musym, mukey', soilmu_p_sql),
        ('soilmu_a', 'mupolygon', 'geom, areasymbol, spatialver, musym, mukey', soilmu_a_sql)
    ]

    temp_sql = SqlParser(file='sql/create_tables_temp.sql')
    temp_stmts = temp_sql.sql
    execute_statements(db=db, stmts=temp_stmts, schema=schema)
    if db.dbtype == 'spatialite':
        paramstr = '?'
    else:
        paramstr = '%s'
    existing = []
    db.open()
    c = db.conn.cursor()
    c.execute("SELECT areasymbol, saversion FROM {schema}.sacatalog ORDER BY areasymbol;"
              .format(**{'schema': schema}))
    for row in c:
        existing.append({'ssa': row[0].lower(), 'version': row[1]})
    db.close()
    # searches scanpath for SSURGO tabular data and imports into dbpath
    import_list = []
    for root, dirs, files in os.walk(scanpath):
        for f in files:
            current_dict = {}
            if f.find('soil_metadata_') >= 0 and f.find('.txt') > 0:
                current_ssa = re.findall(r'soil_metadata_(.*?).txt', f)[0]
                current_dict['root'] = root
                current_dict['ssa'] = current_ssa.lower()
                with open(os.path.join(root, 'tabular', 'sacatlog.txt'), 'r') as textfile:
                    reader = csv.reader(textfile, quotechar='"', delimiter='|')
                    scan_ssa_version = int(next(reader)[2])
                current_dict['vnew'] = scan_ssa_version
                # print("Current SSA is", current_ssa, "version", scan_ssa_version)
                if current_ssa.lower() in survey_areas or survey_areas == '':
                    if current_ssa.lower() in [x['ssa'] for x in existing]:
                        existing_ssa_version = [x['version'] for x in existing if x['ssa'] == current_ssa.lower()][0]
                        current_dict['vold'] = existing_ssa_version
                        if existing_ssa_version < scan_ssa_version:
                            current_dict['status'] = 'replace'
                        else:
                            current_dict['status'] = 'skip'
                    else:
                        current_dict['status'] = 'new'
                        current_dict['vold'] = None
                import_list.append(current_dict)

    skip = [x for x in import_list if x['status'] == 'skip']
    for s in skip:
        print('Skipping SSA', s['ssa'], "found version <= existing version in database.")
    to_delete = [x for x in import_list if x['status'] == 'replace']
    for d in to_delete:
        print('Deleting SSA ', d['ssa'], ' version ', d['vold'], ' in preparation for replacement by version ',
              d['vnew'], '.', sep='')
        delete.delete_ssa(db=db, schema=schema, ssa=d['ssa'])
    to_import = [x for x in import_list if x['status'] != 'skip']
    if to_import:
        print("Disabling indices for bulk insert...")
        si_support = toggle_indices(db=db, schema=schema, drop=True)
        db.open()
        c = db.conn.cursor()
        for i in to_import:
            print("Importing SSA ", i['ssa'], ' version ', i['vnew'], '.', sep='')
            base_dir = os.path.basename(i['root'])
            tab_files = []
            spat_files = []
            for root, dirs, files in os.walk(i['root']):
                for f in files:
                    if os.path.splitext(f)[1] == '.txt':
                        tab_files.append((root, f))
                    elif os.path.splitext(f)[1] == '.shp':
                        spat_files.append((root, f))
            for t in tabular_list:
                path_tuple = [x for x in tab_files if t[0] in x[1]]
                if path_tuple:
                    root = path_tuple[0][0]
                    f = path_tuple[0][1]
                    tbl = t[1]
                    print("\tImporting", tbl, 'from', os.path.join(root.replace(scanpath, '').strip('/\\'), f))
                    c.execute(f"SELECT * FROM {schema}.{tbl} LIMIT 1;")
                    fields = [description[0] for description in c.description]
                    param_sql = ','.join([paramstr] * len(fields))
                    if ignore:
                        sql = f"INSERT INTO {schema}.{tbl} VALUES ({param_sql}) ON CONFLICT DO NOTHING;"
                    else:
                        sql = f"INSERT INTO {schema}.{tbl} VALUES ({param_sql});"
                    with open(os.path.join(root, f), 'r', encoding='utf8') as textfile:
                        dictread = csv.DictReader(textfile, fieldnames=fields, quotechar='"', delimiter='|')
                        for row in dictread:
                            for key, value in row.items():
                                if value == '':
                                    row[key] = None
                            try:
                                c.execute(sql, list(row.values()))
                            except (sqlite.IntegrityError, psycopg2.IntegrityError, pyodbc.IntegrityError):
                                print(sql, '\n', row.values())
                                raise
                    db.conn.commit()
            for s in spatial_list:
                path_tuple = [x for x in spat_files if s[0] in x[1]]
                if path_tuple:
                    root = path_tuple[0][0]
                    f = path_tuple[0][1]
                    tbl = s[1]
                    print("\tImporting table:", tbl, 'from', os.path.join(root.replace(scanpath, '').strip('/\\'), f))
                    # list for converting shapefile.Reader() items to well known text (shapefile.Reader() int,
                    # shapefile.Reader() text, Well Known Text)
                    insert_text = s[2]
                    select_stmt = s[3]
                    shp_table = '_'.join((tbl, 'shp'))
                    param_sql = ','.join((len(insert_text.split()) - 1) * [paramstr])
                    isql = f"INSERT INTO {schema}.{shp_table} ({insert_text}) VALUES ({{wkt}},{param_sql})"
                    sf = shapefile.Reader(os.path.join(root, f))
                    shrecs = sf.shapeRecords()
                    for rec in shrecs:
                        wkt = ''.join(("ST_GeomFromText('", str(pygeoif.geometry.as_shape(rec.shape)), "', 4326)"))
                        sql = isql.format(wkt=wkt)
                        c.execute(sql, rec.record)
                    gsql = f"INSERT INTO {schema}.{tbl} ({insert_text}) " \
                           f"{select_stmt.format(schema=schema, tbl=shp_table)} ON CONFLICT DO NOTHING;"
                    try:
                        c.execute(gsql)
                    except (sqlite.Error, pyodbc.Error, psycopg2.Error):
                        print(gsql)
                        raise
                    c.execute(f"DELETE FROM {schema}.{shp_table};")
                    db.conn.commit()
        db.close()
        print("Enabling indices after bulk insert...")
        si_support = toggle_indices(db=db, schema=schema, drop=False)

    # delete vestige virtual table information from shapefile import
    db.open()
    c = db.conn.cursor()
    shp_list = ['_'.join((i[1], 'shp')) for i in spatial_list]
    for tbl in shp_list:
        if db.dbtype == 'spatialite':
            sql = f"SELECT DiscardGeometryColumn('{tbl}', 'geom');"
            c.execute(sql)
        c.execute(f"DROP TABLE {schema}.{tbl};")
    db.conn.commit()
    db.close()

    if len(to_import) > 0:
        new_imports = True
    else:
        new_imports = False
    return new_imports, si_support


def repair_geom(db, schema, featlist):
    """repairs geometries of final spatial features if that option was selected"""
    db.open()
    c = db.conn.cursor()
        
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
        c.execute(temp_sql)
        c.execute("DELETE FROM {!s};".format(tbl))
        isql = "INSERT INTO {!s} ({!s}) SELECT {!s} FROM {!s}_temp;"\
            .format(tbl, ', '.join((t[1], 'geom')), ', '.join((t[1], 'geom')), t[0])
        c.execute(isql)
    db.conn.commit()
    db.close()


def update_stats(db, schema, featlist):
    db.open()
    c = db.conn.cursor()
    tables = [x[1] for x in tabular_list] + featlist
    for table in tables:
        c.execute(f"ANALYZE {schema}.{table};")
    db.conn.commit()
    db.close()


def add_area(db, schema, featlist, force):
    """Adds geometry measurements to final features"""
    db.open()
    c = db.conn.cursor()
    if not force:
        sql_up = "UPDATE {!s} SET {!s} = {!s} WHERE {!s} IS NULL;"
    else:
        sql_up = "UPDATE {!s} SET {!s} = {!s} /* WHERE {!s} IS NULL */;"

    for f in featlist:
        xf = '.'.join((schema, f))
        print("Adding geometry measurements to " + f)
        if 'poly' in f:
            c.execute(sql_up.format(xf, 'area_ha', 'st_area(geom, True)/10000', 'area_ha'))
        elif 'line' in f:
            c.execute(sql_up.format(xf, 'length_m', 'st_length(geom, True)', 'length_m'))
        elif 'point' in f:
            c.execute(sql_up.format(xf, 'x', 'st_x(st_centroid(geom))', 'x'))
            c.execute(sql_up.format(xf, 'y', 'st_y(st_centroid(geom))', 'y'))
    db.conn.commit()
    db.close()


def toggle_indices(db, schema, drop=False):
    spatial_index_support = True
    index_sql = SqlParser(file='sql/create_indices.sql')
    index_stmts = index_sql.sql
    if not drop:
        execute_statements(db=db, stmts=index_stmts,  schema=schema)
        # Necessary for dealing with sqlite libraries that have been compiled without rtree and thus do not have
        # the spatial index functionality
        if db.dbtype == 'spatialite':
            statements = index_stmts.copy()
            for r in spatialite_regex:
                pattern = re.compile(r['pattern'], re.I)
                statements = [re.sub(pattern, r['repl'], x) for x in statements]
            check_index = [re.sub('CreateSpatialIndex', 'CheckSpatialIndex', x, re.I)
                           for x in statements if 'CreateSpatialIndex' in x]
            check_list = []
            db.open()
            c = db.conn.cursor()
            for stmt in check_index:
                rows = c.execute(stmt).fetchall()
                check_list.append(rows)
            # checks for bad spatial indices
            if all([x[0][0] is None for x in check_list]):
                spatial_index_support = False
                print('Disabling spatial indices, Rtree not compiled with python SQLite library.')
                disable_index = [re.sub('CheckSpatialIndex', 'DisableSpatialIndex', x, re.I) for x in check_index]
                for stmt in disable_index:
                    c.execute(stmt)
            db.close()
    else:
        pattern = re.compile(''.join((r"""CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z"\{\}_]+)\s+ON\s+""",
                                      r"""([A-Za-z"\{\}_]+)?\.?([A-Za-z"\{\}_]+)\s+(?:USING\s+(GIST)\s+)?""",
                                      r"""\(([^\)]+)\);""")), re.I)
        drop_groups = [re.findall(pattern, x) for x in index_stmts]
        if db.dbtype == 'spatialite':
            drop_stmts = []
            for d in drop_groups:
                if d:
                    if d[0][3].lower() == 'gist':
                        drop_stmts.append(f"SELECT DisableSpatialIndex('{d[0][2]}', '{d[0][4]}');")
                    else:
                        drop_stmts.append(f"DROP INDEX IF EXISTS {d[0][0]};")
        else:
            drop_stmts = [f'DROP INDEX IF EXISTS {d[0][1]}.{d[0][0]};' for d in drop_groups if d]
        execute_statements(db=db, stmts=drop_stmts, schema=schema)
    return spatial_index_support


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and import all found SSURGO data '
                                                 'found in separate Soil Survey Area folders downloaded from NRCS into '
                                                 'a SpatiaLite database.')
    # positional arguments
    parser.add_argument('scanpath', help='path to recursively scan for SSURGO files')

    # optional arguments
    dbpath_default = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'soils.sqlite')
    parser.add_argument('-d', '--dbname', default=dbpath_default,
                        help='Either the path to the soils spatialite file database or the name of the'
                             'database in a PostreSQL or SQL Server RDBMS instance to be created/updated.')
    parser.add_argument('-H', '--host', default='localhost',
                        help='The dns name or IP address of the database instance to which to connect '
                             '(only for RDBMS connections).')
    # parser.add_argument('-i', '--instance',
    #                     help='For SQL Sever connections, a named instance can be used instead of the port number '
    #                          '(e.g. SQLEXPRESS).')
    parser.add_argument('-P', '--port',
                        help='The port on which the database instance is accepting connections. For SQL Server, '
                             '--instance maybe be used instead.')
    parser.add_argument('-u', '--user',
                        help='The user name used to connect to the database instance '
                             '(only for RDBMS connections). Not providing --user with a SQL Server connection will '
                             'make the script assume a trusted connection.')
    parser.add_argument('-p', '--password',
                        help='The password used to connect to the database instance. Leaving this blank may require the'
                             ' user to enter the password at a prompt for RDBMS connections.')
    parser.add_argument('-S', '--schema', default='',
                        help='tells the script what schema to use (only for RDBMS connections)')
    parser.add_argument('-t', '--type', choices=['spatialite', 'postgis'], default='spatialite',  # , 'mssql'
                        help='which type of database on which to do the import.')
    parser.add_argument('-e', '--ecosite', action='store_true',
                        help='creates a spatial feature called ecopolygon which shows the aggregate dominant '
                             'ecological sites')
    parser.add_argument('-g', '--groups', nargs=2, metavar=("path/to/ecogroup_meta.csv", "/path/to/ecogroup.csv"),
                        help='paths of the csv files containing the ecological group metadata and the sites '
                             'within each ecogroup (see ecogroups_example.csv and ecogroups_meta_example.csv, '
                             'tab delimited).')
    parser.add_argument('-r', '--repair', action='store_true',
                        help='attempts to repair any faulty geometry in original features and faulty geometries '
                             'produced by the --snap option')
    parser.add_argument('-R', '--restrict', metavar='"path/to/list.csv"',
                        help='restricts imports to just those found in a comma delimited list (see list_example.csv)')
    parser.add_argument('-s', '--snap', default=0, type=float, metavar='grid_size_m',
                        help='the grid size (in meters) to snap features to')
    parser.add_argument('-k', '--skip', nargs='*',
                        help='Table(s) to skip during the import (e.g. cointerp). '
                             'WARNING: skipping tables can be dangerous if they are referenced by a FOREIGN KEY '
                             'CONSTRAINT. Use caution.')
    parser.add_argument('-f', '--force', action='store_true',
                        help='will force the recreation/repair of ecosite and ecogroup layers even if no new imports '
                             'are found')

    args = parser.parse_args()

    # check for valid arguments
    if not os.path.isdir(args.scanpath):
        print("scanpath does not exist. Please choose an existing path to search.")
        quit()
    if args.type == 'spatialite':
        if not os.path.isdir(os.path.dirname(args.dbname)):
            print("dbpath directory does not exist. Please choose an existing path to create db in.")
            quit()

    # check for valid optional arguments
    if args.groups is not None:
        csvmetapath = args.groups[0]
        csvpath = args.groups[1]
        if not os.path.isfile(csvmetapath):
            print(csvmetapath + " does not exist. Please choose an existing comma delimited ecogroup_meta file "
                                "to use.")
            good_csv_paths = False
            quit()
        if not os.path.isfile(csvpath):
            print(csvpath + " does not exist. Please choose an existing comma delimited ecogroup file to use.")
            good_csv_paths = False
            quit()
    else:
        csvmetapath = None
        csvpath = None
    if args.restrict is not None:
        if not os.path.isfile(args.restrict):
            print(args.restrict, "does not exist. Please choose an existing comma delimited SSURGO list to use.")
            quit()

    if args.restrict is not None:
        with open(args.restrict, 'r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            sa = []
            for crow in csvreader:
                if crow[0].strip() != '':
                    sa.append(crow[0].strip().lower())
    else:
        sa = ''
    set_csv_limit()
    my_schema = get_default_schema(args.type, args.schema)
    my_db = DbConnect(dbtype=args.type, user=args.user, dbname=args.dbname, port=args.port,  # instance=args.instance,
                      host=args.host, pwd=args.password)
    is_new, eco, grp = initdb(db=my_db, schema=my_schema)
    has_new_imports, si = scan_insert(db=my_db, schema=my_schema, scanpath=args.scanpath, snap=args.snap,
                                      repair=args.repair, skip=args.skip, survey_areas=sa)

    if (args.repair and has_new_imports) or (args.repair and args.force):
        repair_geom(db=my_db, schema=my_schema, featlist=feature_list)

    if has_new_imports or args.force:
        add_area(db=my_db, schema=my_schema, featlist=feature_list, force=args.force)
            
    if args.ecosite or eco:
        feature_list.append('ecopolygon')
        if has_new_imports or args.force:
            print("Creating custom ecosite tables...")
            custom_parser = SqlParser(file=r'sql/create_custom.sql')
            custom_statements = custom_parser.sql
            if not si:
                custom_statements = [x for x in custom_statements if not re.search(r'\bUSING\s+GIST\b', x, re.I)]
            execute_statements(db=my_db, stmts=custom_statements, schema=my_schema)
            if args.repair:
                repair_geom(db=my_db, schema=my_schema, featlist=['ecopolygon'])
                add_area(db=my_db, schema=my_schema, featlist=['ecopolygon'], force=True)
    if args.groups or grp:
        print("Creating ecogroup tables...")
        table_parser = SqlParser(file=r'sql/create_ecogroup_tables.sql')
        table_statements = table_parser.sql
        if not si:
            table_statements = [x for x in table_statements if not re.search(r'\bUSING\s+GIST\b', x, re.I)]
        execute_statements(db=my_db, stmts=table_statements, schema=my_schema)
        print("Loading ecogroup data from delimited files...")
        create_ecogroups.load_ecogroups(db=my_db, schema=my_schema, csvmetapath=csvmetapath, csvpath=csvpath)
        if has_new_imports or args.force:
            feature_list.append('ecogrouppolygon')
            print("Creating ecogroup views...")
            view_parser = SqlParser(file=r'sql/create_ecogroup_views.sql')
            view_statements = view_parser.sql
            execute_statements(db=my_db, stmts=view_statements, schema=my_schema)
            if args.repair:
                repair_geom(db=my_db, schema=my_schema, featlist=['ecogrouppolygon'])
                add_area(db=my_db, schema=my_schema, featlist=['ecogrouppolygon'], force=True)

    print('Updating table statistics...')
    update_stats(db=my_db, schema=my_schema, featlist=feature_list)
    my_db.close()  # shouldn't be open but just in case.
    print("Script finished.")
