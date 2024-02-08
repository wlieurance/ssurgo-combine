#!/usr/bin/env python3
import win32com.client
import pywintypes  # for error handling
import os
import argparse
import pyodbc
import time
import re

report_list = [
{'key': 1, 'name': 'Map Unit Legend', 'lbl': 'Map Unit Legend'},
{'key': 2, 'name': 'Acreage and Proportionate Extent of the Soils', 'lbl': 'Acreage and Proportionate Extent of the Soils'},
{'key': 4, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Agricultural Disposal of Manure, Food-Processing Waste, and Sewage Sludge'},
{'key': 5, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Agricultural Disposal of Wastewater by Rapid Infiltration and Slow Rate Treatment'},
{'key': 12, 'name': 'Rangeland Productivity', 'lbl': 'Rangeland Productivity'},
{'key': 13, 'name': 'Rangeland Productivity and Plant Composition', 'lbl': 'Rangeland Productivity and Plant Composition'},
{'key': 14, 'name': 'Forestland Productivity', 'lbl': 'Forestland Productivity'},
{'key': 15, 'name': 'Source of Sand and Gravel', 'lbl': 'Source of Sand and Gravel'},
{'key': 16, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Source of Reclamation Material, Roadfill, and Topsoil'},
{'key': 17, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Dwellings and Small Commercial Buildings'},
{'key': 18, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Roads and Streets, Shallow Excavations, and Lawns and Landscaping'},
{'key': 19, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Sewage Disposal'},
{'key': 20, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Landfills'},
{'key': 21, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Haul Roads, Log Landings, and Soil Rutting on Forestland'},
{'key': 22, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Hazard of Erosion and Suitability for Roads on Forestland'},
{'key': 23, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Forestland Planting and Harvesting'},
{'key': 24, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Forestland Site Preparation'},
{'key': 25, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Damage by Fire and Seedling Mortality on Forestland'},
{'key': 26, 'name': 'Engineering Properties', 'lbl': 'Engineering Properties'},
{'key': 27, 'name': 'Physical Soil Properties', 'lbl': 'Physical Soil Properties'},
{'key': 30, 'name': 'Chemical Soil Properties', 'lbl': 'Chemical Soil Properties'},
{'key': 31, 'name': 'Water Features', 'lbl': 'Water Features'},
{'key': 32, 'name': 'Soil Features', 'lbl': 'Soil Features'},
{'key': 33, 'name': 'Taxonomic Classification of the Soils', 'lbl': 'Taxonomic Classification of the Soils'},
{'key': 34, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Camp Areas, Picnic Areas, and Playgrounds'},
{'key': 35, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Paths, Trails, and Golf Fairways'},
{'key': 36, 'name': 'Windbreaks and Environmental Plantings', 'lbl': 'Windbreaks and Environmental Plantings'},
{'key': 37, 'name': 'Prime and Other Important Farmlands', 'lbl': 'Prime and Other Important Farmlands'},
{'key': 40, 'name': 'Component Legend', 'lbl': 'Component Legend'},
{'key': 46, 'name': 'RUSLE2 Related Attributes', 'lbl': 'RUSLE2 Related Attributes'},
{'key': 47, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Ponds and Embankments'},
{'key': 48, 'name': 'Survey Area Data Summary', 'lbl': 'Survey Area Data Summary'},
{'key': 50, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Agricultural Disposal of Wastewater by Irrigation and Overland Flow'},
{'key': 51, 'name': 'Hydric Soils', 'lbl': 'Hydric Soils'},
{'key': 52, 'name': 'WEPS Related Attributes', 'lbl': 'Wind Erosion Prediction System Related Attributes'},
{'key': 53, 'name': 'Map Unit Description (Brief, Generated)', 'lbl': 'Map Unit Description (Brief, Generated)'},
{'key': 54, 'name': 'Template Interpretation Report - 3 Interpretations', 'lbl': 'Irrigation - General and Sprinkler'},
{'key': 55, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Irrigation - Micro'},
{'key': 56, 'name': 'Template Interpretation Report - 2 Interpretations', 'lbl': 'Irrigation - Surface'},
{'key': 1001, 'name': '17 MUD2', 'lbl': 'Map Unit Description (Brief, Tabular)'}
]

report_choices = [x['key'] for x in report_list]


def close_connection(obj, mod=None):
    obj.CloseCurrentDatabase()
    obj.Quit()
    obj = None
    if mod is not None:
        obj.VBE.ActiveVBProject.VBComponents.Remove(mod)
        mod = None


def generate_reports(scanpath, reports, musyms=None, overwrite=False, outfolder=None, filter_text='', list=False,
                     sa=None):
    # ac = win32com.client.gencache.EnsureDispatch("Access.Application")
    ac = win32com.client.Dispatch("Access.Application")
    # ac.Visible = False
    dbs = 0
    rpts = 0
    filter_skip = []
    for root, dirs, files in os.walk(scanpath):
        for f in files:
            if os.path.splitext(f)[1] in ['.mdb', '.accdb']:
                if filter_text in f:
                    if outfolder is None:
                        outf = root
                    else:
                        outf = outfolder
                    empty = False
                    not_ssurgo = False
                    notcom = False
                    sa_text = None
                    avail_reports = []
                    file_path = os.path.join(root, f)
                    con = pyodbc.connect(r'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};'.format(file_path))
                    c = con.cursor()
                    try:
                        c.execute("SELECT areasymbol, saversion FROM sacatalog;")
                    except pyodbc.ProgrammingError:
                        not_ssurgo = True
                    else:
                        if not list:
                            print('Exporting reports in', file_path)
                        rows = c.fetchall()
                        if rows:
                            soil_area = rows[0][0]
                            soil_version = rows[0][1]
                            sa_int_str = re.findall(r"^([a-zA-Z]{2})(\d{3})$", soil_area)
                            if sa_int_str:
                                sa_state = sa_int_str[0][0]
                                sa_int = int(sa_int_str[0][1])
                            else:
                                sa_state = None
                                sa_int = None
                            if sa is not None:
                                if sa[1] and sa[2]:
                                    test = "'{}{}' {} '{}{}'".format(sa_state, sa_int, sa[0], sa[1], sa[2])
                                elif sa[1]:
                                    test = "'{}' {} '{}'".format(sa_state, sa[0], sa[1])
                                elif sa[2]:
                                    test = "{} {} {}".format(sa_int, sa[0], sa[2])
                                else:
                                    test = "False"
                            else:
                                test = "True"
                            if eval(test):
                                sa_text = '_v'.join((soil_area, str(soil_version)))
                                if len(rows) > 1:
                                    sa_text += '_etal'
                                c.execute("SELECT * FROM mapunit")
                                mus = c.fetchall()
                                if len(mus) <= 1:
                                    notcom = True
                                report_sql = "SELECT [Report Key], [Access Report Name], [Report Name] " \
                                             "  FROM [SYSTEM - Soil Reports] " \
                                             " WHERE [Include Report] = True " \
                                             "   AND [Parameters Required]=False"
                                avail_reports = c.execute(report_sql).fetchall()
                                if list:
                                    report_text = os.linesep.join(
                                        [''.join([''.join((str(x[0]) + ':')).ljust(10), x[2]]) for x in
                                         avail_reports])
                                    print(report_text, '\n')
                                    con.close()
                                    return 0, 0, None
                                avail_keys = [x[0] for x in avail_reports]
                                in_reports = [r for r in reports if r in avail_keys]
                                out_reports = [r for r in reports if r not in avail_keys]
                                if out_reports:
                                    print('MISSING REPORT KEYS: [',
                                          ', '.join([str(x) for x in out_reports]), '] in ', file_path, sep='')
                        else:
                            empty = True
                    con.close()
                    if not (not_ssurgo or notcom or empty) and eval(test):
                        try:
                            ac.OpenCurrentDatabase(file_path)
                        except pywintypes.com_error:
                            close_connection(obj=ac)
                            raise
                        # ac.DoCmd.SetWarnings(False)
                        dbs += 1
                        time.sleep(3)  # needed to prevent race condition during adding of new module 'temp'
                        # need to insert modules to set Public Variables that the reports need
                        ac.VBE.ActiveVBProject.VBComponents.Add(1).Name = "temp"
                        temp_module = ac.VBE.ActiveVBProject.VBComponents.Item("temp")
                        mod_text = os.linesep.join((
                            "Public Function SetvrKey(vrKey As Variant)",
                            "varReportKey = vrKey",
                            "End Function",
                            "",
                            "Public Function SetInclude(IncSoils As Variant, IncRD As Variant) ",
                            "blnIncludeMinorSoils = IncSoils",
                            "blnIncludeReportDescription = IncRD",
                            "End Function"
                        ))
                        ac.Modules('temp').InsertText(mod_text)
                        # Sets public vars for Include Minor Soils and Include Report Description
                        result = ac.Run("SetInclude", True, False)
                        ac.DoCmd.OpenForm('Soil Reports')
                        ac.Forms('Soil Reports').Controls('tglIncludeMinorSoils').Value = True
                        ac.Forms('Soil Reports').Controls('tglIncludeReportDescription').Value = True
                        ac.DoCmd.RunSQL("DELETE * FROM [SYSTEM - Selected Legend Key]")
                        ac.DoCmd.RunSQL("DELETE * FROM [SYSTEM - Selected Mapunit Keys]")
                        ac.DoCmd.RunSQL("INSERT INTO [SYSTEM - Selected Legend Key] (lkey) "
                                        "SELECT lkey FROM [Select List - Legend]")
                        mu_sql = "INSERT INTO [SYSTEM - Selected Mapunit Keys] (mukey) " \
                                 "SELECT mukey FROM [Select List - Mapunit]"
                        if musyms is not None:
                            stringlist = ["'" + x + "'" for x in musyms]
                            mustring = ', '.join(stringlist)
                            mu_sql = ' '.join((mu_sql, 'WHERE musym IN ({})')).format(mustring)
                        ac.DoCmd.RunSQL(mu_sql)
                        for report in in_reports:
                            report_name = [x[1] for x in avail_reports if x[0] == report][0]
                            report_lbl = [x[2] for x in avail_reports if x[0] == report][0]
                            lbl_frmt = re.sub(r'\s*[\.\\/]\s*', '_', report_lbl)
                            report_file = ''.join((sa_text, "_", lbl_frmt, ".pdf"))
                            report_path = os.path.join(outf, report_file)
                            if (os.path.isfile(report_path) and overwrite) or not os.path.isfile(report_path):
                                print('\t', report_lbl)
                                result = ac.Run("SetvrKey", report)  # sets the key for the report generation
                                try:
                                    ac.DoCmd.OutputTo(3, report_name, "PDF Format (*.pdf)", report_path, False)
                                except pywintypes.com_error:
                                    close_connection(obj=ac, mod=temp_module)
                                    raise
                                except KeyboardInterrupt:
                                    close_connection(obj=ac, mod=temp_module)
                                    raise
                                else:
                                    rpts += 1
                            else:
                                print('\t', report_file, 'already exists. Skipping...')
                        ac.VBE.ActiveVBProject.VBComponents.Remove(temp_module)
                        temp_module = None
                        # ac.DoCmd.SetWarnings(True)
                        ac.CloseCurrentDatabase()
                    else:
                        if not_ssurgo:
                            print(file_path, "does not appear to be SSURGO template DB. Skipping...")
                        elif notcom:
                            print(file_path, "Soil survey is not completed. Skipping... ")
                        elif empty:
                            print(file_path, "does not appear to be populated. Skipping...")
                        elif not eval(test):
                            print(file_path, "Skipped by manual filtering (--sa_filter)")
                        else:
                            print("Unknown skip reason.")
                else:
                    filter_skip.append(os.path.join(root, f))
    ac.Quit()
    ac = None
    return rpts, dbs, filter_skip


if __name__ == "__main__":
    # parses script arguments
    report_text = os.linesep.join([''.join([''.join((str(x['key']) + ':')).ljust(10), x['lbl']]) for x in report_list])
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description=os.linesep.join((
                                         'This script will scan a folder path for populated MS Access SSURGO soil '
                                         'databases and exported selected reports for selected map units.'
                                         'ALL OPEN MS ACCESS DATABASES NEED TO BE CLOSED BEFORE THE SCRIPT IS RUN TO '
                                         'PREVENT ERRORS.', '',
                                         'Available reports for the standard US SSURGO MS Access template are listed '
                                         'below. State template specific keys can be used but are not listed as keys '
                                         'can refer to different reports in different templates. Please '
                                         'use the report_keys.py script to list the available report keys for a '
                                         'specific database. The --filter option can then be used to operate on only '
                                         'those state template databases.', '',
                                         '{}'.format(report_text))))
    # positional arguments
    parser.add_argument('scanpath', help='path to recursively scan for populated MS Access SSURGO databases.')
    regular = parser.add_mutually_exclusive_group()
    regular.add_argument('-r', '--report', nargs='+',  type=int,
                        help='the report key(s) of the MS Access report to export.')
    regular.add_argument('-l', '--list', action='store_true',
                        help='list the report keys and descriptions of the first MS Access report to be encountered '
                             'on scanpath.')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite reports if a file of the same name already exists.')
    parser.add_argument('-p', '--out_path',
                        help='The folder to output reports to (defaults will be same directory in which database is '
                             'located.)')
    parser.add_argument('-f', '--filter', default='',
                        help='A text string which must be present in the file name of the MS Access database to '
                             'process it.')
    parser.add_argument('-F', '--sa_filter', help='A text string in the format of "[!<>=]=(ST)###" where "ST" is the '
                                                  'state abbreviation (optional) and ### is the soil area number which '
                                                  'to filter above, below, or at. (also optional, though one or the '
                                                  'other must be given)')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-m', '--musym', nargs='+',
                       help='map unit symbol(s) to which the report is restricted.')
    group.add_argument('-M', '--musym_file',
                       help='The path to a text file containing map unit symbols to which the reports is restricted, '
                            'one musym per line.')
    args = parser.parse_args()

    # check for valid arguments
    if not os.path.isdir(args.scanpath):
        print(args.scanpath, "does not exist. Please choose an existing path to search.")
        quit()
    if args.musym_file is not None:
        if not os.path.isfile(args.musym_file):
            print(args.musym_file, "does not exist. Please choose an existing file.")
            quit()
    if args.out_path is not None:
        if not os.path.isdir(args.out_path):
            print(args.out_path, "does not exist. Please choose an existing directory.")
            quit()
    if args.sa_filter is not None:
        good = re.findall(r"^([<>!=]?=?)([A-Za-z]{2})*(\d{1,3})*$", args.sa_filter)
        if not good:
            print("--sa_filter object not correctly formed.")
            quit()
        else:
            sa = (good[0][0], good[0][1], int(good[0][2]))
    else:
        sa = None
    if args.musym is not None:
        musyms = args.musym
    elif args.musym_file is not None:
        with open(args.musym_file) as f:
            musyms = f.readlines()
            musyms = [x.strip() for x in musyms if x.strip()]
    else:
        musyms = None
    i, j, s = generate_reports(scanpath=args.scanpath, reports=args.report, musyms=musyms, overwrite=args.overwrite,
                               outfolder=args.out_path, filter_text=args.filter, list=args.list, sa=sa)
    print('Exported', i, 'reports from', j, 'soil databases.')
    if s:
        print('Skipped the following databases due to filter option:')
        for skp in s:
            print(skp)
    print(os.linesep)
    print('Script finished.')