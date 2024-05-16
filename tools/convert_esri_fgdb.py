import argparse
import sys
import os
import sqlite3
import pandas as pd
import json
import arcpy

db_path = r'H:\temp\mlra27_soil.sqlite'
out_dir = r'H:\temp'
cxn = sqlite3.connect(db_path)
cxn.enable_load_extension(True)
cxn.execute("SELECT load_extension('mod_spatialite');")
# cxn.conn.execute("PRAGMA foreign_keys = ON;")

fgdb_path = r"H:\temp\temp.gdb"
if not os.path.isdir(fgdb_path):
    arcpy.management.CreateFileGDB(
        out_folder_path=os.path.dirname(fgdb_path),
        out_name=os.path.basename(fgdb_path),
        out_version="CURRENT"
    )

tabular_list = [
    'mdstatdommas',
    'mdstattabs',
    'sacatalog',
    'sdvalgorithm',
    'sdvattribute',
    'sdvfolder',
    'featdesc',
    'legend',
    'mdstatdomdet',
    'mdstatidxmas',
    'mdstatrshipmas',
    'mdstattabcols',
    'sdvfolderattribute',
    'distlegendmd',
    'distmd',
    'distinterpmd',
    'laoverlap',
    'legendtext',
    'mapunit',
    'mdstatidxdet',
    'mdstatrshipdet',
    'sainterp',
    'component',
    'muaggatt',
    'muaoverlap',
    'mucropyld',
    'mutext',
    'chorizon',
    'cocanopycover',
    'cocropyld',
    'codiagfeatures',
    'coecoclass',
    'coeplants',
    'coerosionacc',
    'coforprod',
    'cogeomordesc',
    'cohydriccriteria',
    'cointerp',
    'comonth',
    'copmgrp',
    'copwindbreak',
    'corestrictions',
    'cosurffrags',
    'cotaxfmmin',
    'cotaxmoistcl',
    'cotext',
    'cotreestomng',
    'cotxfmother',
    'chaashto',
    'chconsistence',
    'chdesgnsuffix',
    'chfrags',
    'chpores',
    'chstructgrp',
    'chtext',
    'chtexturegrp',
    'chunified',
    'coforprodo',
    'copm',
    'cosoilmoist',
    'cosoiltemp',
    'cosurfmorphgc',
    'cosurfmorphhpp',
    'cosurfmorphmr',
    'cosurfmorphss',
    'chstruct',
    'chtexture',
    'chtexturemod',
]

for tbl in tabular_list:
    sql = f'SELECT * FROM {tbl};'
    json_path = os.path.join(out_dir, f'{tbl}.json')
    csv_path = os.path.join(out_dir, f'{tbl}.csv')
    df = pd.read_sql_query(sql, cxn)
    df.spatial.to_table(os.path.join(fgdb_path, tbl))
    # df.to_json(outpath, orient='records', lines=True)
    df.to_csv(csv_path, index=False)

    recs = df.to_records(index=False)
    arcpy.da.NumPyArrayToTable(recs, os.path.join(fgdb_path, tbl))

    arcpy.conversion.JSONToFeatures(
        in_json_file=outpath,
        out_features=os.path.join(fgdb_path, tbl),
        geometry_type="POLYGON"
    )




feature_list = ['featline', 'featpoint', 'muline', 'mupoint', 'mupolygon', 'sapolygon']