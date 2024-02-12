
import csv
import argparse
import sys

# custom
import import_soil
from parse_sql import *


def load_ecogroups(db, schema, csvmetapath, csvpath):
    db.open()
    c = db.conn.cursor()
    if db.dbtype == 'spatialite':
        paramstr = '?'
        ins1 = 'OR IGNORE'
        ins2 = ''
    else:
        paramstr = '%s'
        ins1 = ''
        ins2 = 'ON CONFLICT DO NOTHING'
    if csvmetapath is not None and csvpath is not None:
        for name, path in [('ecogroup', csvpath), ('ecogroup_meta', csvmetapath)]:
            c.execute(f"DELETE FROM {schema}.{name};")
        for name, path in [('ecogroup_meta', csvmetapath), ('ecogroup', csvpath)]:
            with open(path, 'r', newline='') as csvfile:
                dialect = csv.Sniffer().sniff(csvfile.read(1024), delimiters=",\t|")
                csvfile.seek(0)
                csvreader = csv.reader(csvfile, dialect)
                headers = next(csvreader)
                header_sql = ', '.join(headers)
                param_sql = ', '.join([paramstr]*len(headers))
                sql = f"INSERT {ins1} INTO {schema}.{name} ({header_sql}) VALUES ({param_sql}) {ins2};"
                for row in csvreader:
                    if row:
                        convert = [f.strip() if f.strip() else None for f in row]
                        c.execute(sql, convert)
            db.conn.commit()
    else:
        print("Missing either ecogroup_meta path or ecogroup path. Both must be provided. Skipping data load...")
    db.close()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will add ecological groupings to an already created '
                                                 'SSURGO database. The ecogroups table will be created and populated '
                                                 'via a comma delimited file, and the ecogrouppolygon spatial feature '
                                                 'will be created.')
    # positional arguments
    parser.add_argument('csvmetapath',  metavar="path/to/ecogroup_meta.csv",
                        help='path of the csv files containing the ecological group metadata (see '
                             'examples/ecogroups_meta_example.csv). Tab, comma, or pipe delimited.')
    parser.add_argument('csvpath',  metavar="path/to/ecogroup.csv",
                        help='path of the csv files containing the ecological sites within each group (see '
                             'examples/ecogroup_example.csv). Tab, comma, or pipe delimited.')
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
    geo = parser.add_argument_group('Geometry options')
    geo.add_argument('-t', '--spatial_type', choices=['sf', 'esri'], default='sf',
                     help='Polygon construction method. "sf" will use the ISO 19125-1 OGC Simple Features standard '
                          '(CCW), and "esri" will use the ESRI/Arc standard (CW).')

    my_args = sys.argv[1:]
    args = parser.parse_args(my_args)

    if args.dbpath is None and args.dbname is not None:
        dbtype = 'postgis'
    elif args.dbpath is not None and args.dbname is None:
        dbtype = 'spatialite'
    else:
        dbtype = None

    # choose spatial direction:
    if args.spatial_type == 'esri':
        poly_dir = 'ST_ForcePolygonCW'
    else:
        poly_dir = 'ST_ForcePolygonCCW'

    # check for valid arguments
    if dbtype == 'spatialite':
        if not os.path.isfile(args.dbpath):
            print("dbpath does not exist. Please choose an existing SSURGO file to use.")
            quit()
    if not os.path.isfile(args.csvmetapath):
        print(args.csvmetapath + " does not exist. Please choose an existing comma delimited ecogroup_meta file to "
                                 "use.")
        quit()
    if not os.path.isfile(args.csvpath):
        print(args.csvpath + " does not exist. Please choose an existing comma delimited ecogroup file to use.")
        quit()
    
    my_schema = import_soil.get_default_schema(dbtype, args.schema)
    my_db = import_soil.DbConnect(dbtype=dbtype, dbpath=args.dbpath, user=args.user, dbname=args.dbname, port=args.port,
                                  host=args.host, pwd=args.password)
    print("Creating tables...")
    table_parser = SqlParser(file=r'sql/create_ecogroup_tables.sql')
    table_statements = table_parser.sql
    import_soil.execute_statements(db=my_db, stmts=table_statements, schema=my_schema)
    print("Loading ecogroup data from delimited files...")
    load_ecogroups(db=my_db, schema=my_schema, csvmetapath=args.csvmetapath, csvpath=args.csvpath)
    print("Creating views...")
    view_parser = SqlParser(file=r'sql/create_ecogroup_views.sql')
    view_statements = view_parser.sql
    import_soil.execute_statements(db=my_db, stmts=view_statements, schema=my_schema, params={'st_direction': poly_dir})
    print('Script finished.')
