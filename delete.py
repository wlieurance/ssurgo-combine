#!/usr/bin/env python3
import os
import argparse
import sys
# custom
import import_soil


def remove_custom(db, schema):
    db.open()
    c = db.conn.cursor()
    sql = "DROP {type} IF EXISTS {schema}.{item};"
    itemlist = [
        {'type': 'table', 'item': 'ecopolygon'},
        {'type': 'view', 'item': 'ecogroup_area'},
        {'type': 'view', 'item': 'ecogroup_mudominant'},
        {'type': 'view', 'item': 'ecogroup_plantprod'},
        {'type': 'view', 'item': 'ecogroup_unique'},
        {'type': 'view', 'item': 'ecogroup_wide'},
        {'type': 'view', 'item': 'ecogroup_mapunit_ranked'},
        {'type': 'view', 'item': 'ecogroup_detail'},
        {'type': 'table', 'item': 'ecogrouppolygon'},
        {'type': 'table', 'item': 'ecogroup'},
        {'type': 'table', 'item': 'ecogroup_meta'}]
    for item in itemlist:
        i = item.copy()
        i['schema'] = schema
        print("DROPPING", item['type'], item['item'])
        c.execute(sql.format(**i))
    db.conn.commit()
    db.close()


def delete_ssa(db, schema, ssa):
    db.open()
    c = db.conn.cursor()
    if ssa[0] == 'all':
        print('CASCADE DELETING ALL Soil Survey Areas')
        c.execute("DELETE FROM {schema}.sacatalog;".format(**{'schema': schema}))
    else:
        sql = "DELETE FROM {schema}.sacatalog WHERE lower(areasymbol) = '{area}';"
        for area in ssa:
            print("CASCADE DELETING Soil Survey Area:", area)
            c.execute(sql.format(**{'schema': schema, 'area': area.lower()}))
    db.conn.commit()
    db.close()


def delete_meta(db, schema):
    db.open()
    c = db.conn.cursor()
    metalist = ['mdstatdommas', 'mdstattabs', 'month', 'sdvalgorithm', 'sdvattribute', 'sdvfolder']
    for meta in metalist:
        print('CASCADE DELETING', meta)
        c.execute("DELETE FROM {schema}.{meta};".format(**{'schema': schema, 'meta': meta}))
    db.conn.commit()
    db.close()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script can remove imported Soil Areas, custom features and user'
                                                 'data from a database previously created with the extract function.')
    # positional arguments
    dbo = parser.add_argument_group('Database options')
    dbo_grp = dbo.add_mutually_exclusive_group(required=True)
    dbo_grp.add_argument('-D', '--dbpath',
                         help='File path for the SpatiaLite database to be created/updated.')
    dbo_grp.add_argument('-d', '--dbname',
                         help='Name of the database in a PostreSQL instance to be created/updated.')
    postgres = parser.add_argument_group('PostGIS options')
    postgres.add_argument('-H', '--host', default='localhost',
                          help='DNS name or IP address of the database instance to which to connect')
    # parser.add_argument('-i', '--instance',
    #                     help='For SQL Sever connections, a named instance can be used instead of the port number '
    #                          '(e.g. SQLEXPRESS).')
    postgres.add_argument('-P', '--port',
                          help='Port on which the database instance is accepting connections.')
    postgres.add_argument('-u', '--user',
                          help='User name used to connect to the database instance')
    postgres.add_argument('-p', '--password',
                          help='Password used to connect to the database instance. Leaving this blank may require '
                               'the user to enter the password at a prompt.')
    postgres.add_argument('-S', '--schema', default='',
                          help='Schema to use within database')
    delete = parser.add_argument_group('Deletion options')
    delete_ex = delete.add_mutually_exclusive_group(required=True)
    delete_ex.add_argument('-x', '--delete', action='store_true',
                           help='Deletes custom ecosite and ecogroup tables.')
    delete_ex.add_argument('-a', '--ssa', nargs='+',
                           help='deletes specific soil survey areas from the database, as listed in the '
                                '"ssa" field in the "legend" table (case insensitive).')
    delete_ex.add_argument('-m', '--meta', action='store_true',
                           help='deletes non-ssa specific metadata loaded into the database in the following tables:'
                                '("mdstatdommas", "mdstattabs", "month", "sdvalgorithm", "sdvattribute", "sdvfolder")')
    delete_ex.add_argument('-w', '--wipe', action='store_true',
                           help='removes all data from the database.')

    my_args = sys.argv[1:]
    args = parser.parse_args(my_args)

    if args.dbpath is None and args.dbname is not None:
        dbtype = 'postgis'
    elif args.dbpath is not None and args.dbname is None:
        dbtype = 'spatialite'
    else:
        dbtype = None

    if dbtype == 'spatialite':
        if not os.path.isfile(args.dbpath):
            print("Sqlite database does not exist. Choose existing database.")
            quit()

    my_schema = import_soil.get_default_schema(dbtype, args.schema)
    my_db = import_soil.DbConnect(dbtype=dbtype, dbpath=args.dbpath, user=args.user, dbname=args.dbname, port=args.port,
                                  host=args.host, pwd=args.password)
    if args.delete:
        remove_custom(db=my_db, schema=my_schema)
    if args.wipe:
        ssa = ['all']
    else:
        ssa = args.ssa
    if args.ssa or args.wipe:
        delete_ssa(db=my_db, schema=my_schema, ssa=ssa)
    if args.meta or args.wipe:
        delete_meta(db=my_db, schema=my_schema)
