#!/usr/bin/env python3
import os
import argparse

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
    if ssa == 'all':
        print('CASCADE DELETING ALL Soil Survey Areas')
        c.execute("DELETE FROM {schema}.sacatalog;".format(**{'schema': schema}))
    else:
        ssas = [x.strip() for x in ssa.split(',')]
        sql = "DELETE FROM {schema}.sacatalog WHERE lower(areasymbol) = '{area}';"
        for area in ssas:
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
    parser.add_argument('-d', '--dbname',
                        help='Either the path to the soils spatialite file database or the name of the'
                             'database in a PostreSQL or SQL Server RDBMS instance to be created/updated.')
    parser.add_argument('-H', '--host', default='localhost',
                        help='The dns name or IP address of the database instance to which to connect '
                             '(only for RDBMS connections).')
    parser.add_argument('-i', '--instance',
                        help='For SQL Sever connections, a named instance can be used instead of the port number '
                             '(e.g. SQLEXPRESS).')
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
    parser.add_argument('-t', '--type', choices=['spatialite', 'postgis', 'mssql'], default='spatialite',
                        help='which type of database on which to do the import.')
    parser.add_argument('-d', '--delete', action='store_true',
                        help='deletes data from all custom tables in features and drops tables.')
    parser.add_argument('-a', '--ssa', help='deletes specific soil survey areas from the database, as listed in the '
                                            '"ssa" field in the "legend" table (case insensitive). Provide comma '
                                            'separated list of soil survey areas to delete')
    parser.add_argument('-m', '--meta', action='store_true',
                        help='deletes non-ssa specific metadata loaded into the database')
    parser.add_argument('-w', '--wipe', action='store_true',
                        help='removes all data from the database.')
    args = parser.parse_args()

    if not (args.delete or args.ssa or args.meta or args.wipe):
        print(os.linesep + "need one type of deletion provided (delete, ssa, ecosite, or groups) "
                           "to run this script." + os.linesep)
        quit()

    if args.type == 'spatialite':
        if not os.path.isfile(args.dbpath):
            print("Sqlite database does not exist. Choose existing database.")
            quit()

    schema = import_soil.get_default_schema(args.type, args.schema)
    my_db = import_soil.DbConnect(dbtype=args.type, user=args.user, dbname=args.dbname, port=args.port,
                                  instance=args.instance, host=args.host, pwd=args.password)
    if args.delete:
        remove_custom(db=my_db, schema=schema)
    if args.wipe:
        ssa = 'all'
    else:
        ssa = args.ssa
    if args.ssa or args.wipe:
        delete_ssa(db=my_db, schema=schema, ssa=ssa)
    if args.meta or args.wipe:
        delete_meta(db=my_db, schema=schema )
