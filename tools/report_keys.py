#!/usr/bin/env python3
import argparse
import pyodbc
import os


def get_reports(path):
    con = pyodbc.connect(r'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};'.format(path))
    c = con.cursor()
    report_sql = "SELECT [Report Key], [Access Report Name], [Report Name] " \
                 "  FROM [SYSTEM - Soil Reports] " \
                 " WHERE [Include Report] = True " \
                 "   AND [Parameters Required]=False"
    rows = c.execute(report_sql).fetchall()
    return rows


def print_reports(reports):
    max_key = len(str(max([x[0] for x in reports])))
    header = [('Key', 'Object Name', 'Report Name')] + reports
    report_text = os.linesep.join([''.join([''.join((str(x[0]) + ':')).ljust(max_key + 2), x[2]]) for x in header])
    print(os.linesep)
    print(report_text)
    print(os.linesep)


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will list available parameter free report names and keys '
                                                 'for an MS Access SSURGO soil database.')
    # positional arguments
    parser.add_argument('path', help='path to an MS Acesss database.')
    args = parser.parse_args()

    # check for valid arguments
    if not os.path.isfile(args.path):
        print(args.path, "does not exist. Please choose an existing path to search.")
        quit()
    if os.path.splitext(args.path)[1] not in ['.mdb', '.accdb']:
        print(args.path, "is not an MS Access file [*.mdb, *.accdb].")
        quit()

    avail_reports = get_reports(path=args.path)
    print_reports(avail_reports)
