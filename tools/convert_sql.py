# must be run via `python -m tools.convert_sql` do to importing from parent directory
import argparse
import sys
from config import spatialite_regex, mssql_regex
from import_soil import get_default_schema
from parse_sql import *


def convert_statements(dbtype, stmts, schema, params={}):
    statements = stmts.copy()
    if dbtype == 'spatialite':
        regex_list = spatialite_regex
    elif dbtype == 'mssql':
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
        print(stmt)
        print('\n')


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will print out formatted sql statements in either postgis'
                                                 ' or spatialite format.')
    # positional arguments
    parser.add_argument('sqlpath', help='path to a file with sql statements.')
    parser.add_argument('-d', '--dbtype', choices=['postgis', 'spatialite', 'mssql'], default='postgis',
                        help='the database for which the sql will be formatted.')
    parser.add_argument('-s', '--schema', default='',
                        help='Schema to use within database')
    parser.add_argument('-t', '--spatial_type', choices=['sf', 'esri'], default='sf',
                        help='Polygon construction method. "sf" will use the ISO 19125-1 OGC Simple Features standard '
                             '(CCW), and "esri" will use the ESRI/Arc standard (CW).')
    my_args = sys.argv[1:]
    args = parser.parse_args(my_args)

    # choose spatial direction:
    if args.spatial_type == 'esri':
        poly_dir = 'ST_ForcePolygonCW'
    else:
        poly_dir = 'ST_ForcePolygonCCW'

    parser = SqlParser(file=args.sqlpath)
    statements = parser.sql
    my_schema = get_default_schema(dbtype=args.dbtype, schema=args.schema)
    convert_statements(dbtype=args.dbtype, stmts=statements, schema=my_schema, params={'st_direction': poly_dir})
