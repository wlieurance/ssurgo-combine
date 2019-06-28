#!/usr/bin/env python3
import sqlite3 as sqlite
import psycopg2
import os
import pyodbc
import argparse
import extract


def delete_custom(dbpath, schema, dbtype, custom):
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
    if dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
    deltbls = [x.strip() for x in custom.split(',')]
    sql = "DELETE FROM {schema}.{table};"
    for tbl in deltbls:
        print("DELETING ROWS FROM", tbl)
        c.execute(sql.format(**{'schema': schema, 'table': tbl}))
    conn.commit()


def remove_custom(dbpath, schema, dbtype, remove):
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
    if dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
    sql = "DROP {type} IF EXISTS {schema}.{item};"
    itemlist = []
    if remove in ['ecosite', 'all']:
        itemlist.append({'type': 'table', 'item': 'ecopolygon'})
    if remove in ['ecogroup', 'all']:
        itemlist.append({'type': 'view', 'item': 'ecogroup_area'})
        itemlist.append({'type': 'view', 'item': 'ecogroup_mudominant'})
        itemlist.append({'type': 'view', 'item': 'ecogroup_plantprod'})
        itemlist.append({'type': 'view', 'item': 'ecogroup_unique'})
        itemlist.append({'type': 'view', 'item': 'ecogroup_wide'})
        itemlist.append({'type': 'view', 'item': 'ecogroup_mapunit_ranked'})
        itemlist.append({'type': 'view', 'item': 'ecogroup_detail'})
        itemlist.append({'type': 'table', 'item': 'ecogrouppolygon'})
        itemlist.append({'type': 'table', 'item': 'ecogroup'})
        itemlist.append({'type': 'table', 'item': 'ecogroup_meta'})
    for item in itemlist:
        i = item
        i['schema'] = schema
        print("DROPPING", item['type'], item['item'])
        c.execute(sql.format(**i))
    conn.commit()


def delete_ssa(dbpath, schema, dbtype, ssa):
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
        c.execute("PRAGMA foreign_keys = ON;")
    if dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
    if ssa == 'all':
        print('CASCADE DELETING ALL Soil Survey Areas')
        c.execute("DELETE FROM {schema}.sacatalog;".format(**{'schema': schema}))
    else:
        ssas = [x.strip() for x in ssa.split(',')]
        sql = "DELETE FROM {schema}.sacatalog WHERE lower(areasymbol) = '{area}';"
        for area in ssas:
            print("CASCADE DELETING Soil Survey Area:", area)
            c.execute(sql.format(**{'schema': schema, 'area': area.lower()}))
    conn.commit()


def delete_meta(dbpath, schema, dbtype):
    if dbtype == 'spatialite':
        conn = sqlite.connect(dbpath)
        conn.enable_load_extension(True)
        c = conn.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")
        c.execute("PRAGMA foreign_keys = ON;")
    if dbtype == 'postgis':
        conn = psycopg2.connect(dbpath)
        c = conn.cursor()
    metalist = ['mdstatdommas', 'mdstattabs', 'month', 'sdvalgorithm', 'sdvattribute', 'sdvfolder']
    for meta in metalist:
        print('CASCADE DELETING', meta)
        c.execute("DELETE FROM {schema}.{meta};".format(**{'schema': schema, 'meta': meta}))
    conn.commit()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script can remove imported Soil Areas, custom features and user'
                                                 'data from a database previously created with the extract function.')
    # positional arguments
    parser.add_argument('dbpath', metavar='CONNECTION_STRING',
                        help='if db type is spatialite, then this is the path to the SpatialLite database to import '
                             'into, (e.g. ("path/to/db.sqlite") which will be created if does not exist. Otherwise, '
                             'this is the database connection string for your database. (e.g. "dbname=somedb '
                             'user=someuser password=\'somepass\'" for postgis')
    # or "server=SOME\SQLSERVER;database=somedb;trusted_connection=yes" for mssql)

    # optional arguments
    parser.add_argument('-x', '--schema', default='',
                        help='tells the script what schema to use (do not use for spatialite, or use "main" '
                             'for schema name)')
    parser.add_argument('-t', '--type', metavar='DATABASE_TYPE', default='spatialite',
                        help='decides which database type to import to: spatialite, postgis') #, or mssql
    parser.add_argument('-d', '--delete', nargs='?', const='ecogroup,ecogroup_meta,ecopolygon,ecogrouppolygon',
                        help='deletes data from all custom tables in features. provide comma separated list for '
                             'choosing, specific tables')
    parser.add_argument('-e', '--ecosite', action='store_true',
                        help='removes the custom spatial feature called ecopolygon which shows the aggregate dominant '
                             'ecological sites')
    parser.add_argument('-g', '--groups', action='store_true',
                        help='removes the custom spatial feature called ecogrouppolygon, the user uploaded tables of '
                             '"ecogroup_meta" and "ecogroup", and any associated ecogroup based views')
    parser.add_argument('-a', '--ssa', help='deletes specific soil survey areas from the database, as listed in the '
                                            '"ssa" field in the "legend" table (case insensitive). Provide comma '
                                            'separated list of soil survey areas to delete')
    parser.add_argument('-m', '--meta', action='store_true',
                        help='deletes non-ssa specific metadata loaded into the database')
    parser.add_argument('-w', '--wipe', action='store_true',
                        help='removes all data from the database.')
    args = parser.parse_args()

    if not (args.delete or args.ssa or args.ecosite or args.groups or args.meta or args.wipe):
        print(os.linesep + "need one type of deletion provided (delete, ssa, ecosite, or groups) "
                           "to run this script." + os.linesep)
        quit()

    if args.type == 'spatialite':
        if not os.path.isfile(args.dbpath):
            print("Sqlite database does not exist. Choose existing database.")
            quit()

    schema = extract.get_default_schema(args.type, args.schema)
    if args.delete:
        delete_custom(args.dbpath, schema, args.type, args.delete)
    if args.ecosite or args.wipe:
        remove_custom(args.dbpath, schema, args.type, 'ecosite')
    if args.groups or args.wipe:
        remove_custom(args.dbpath, schema, args.type, 'ecogroup')
    if args.wipe:
        ssa = 'all'
    else:
        ssa = args.ssa
    if args.ssa or args.wipe:
        delete_ssa(args.dbpath, schema, args.type, ssa)
    if args.meta or args.wipe:
        delete_meta(args.dbpath, schema, args.type)
