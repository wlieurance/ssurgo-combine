#!/usr/bin/env python3
import os
import csv
import argparse
import shutil
import re
import copy


def get_meta(scanpath, verbose=False):
    cat_paths = []
    for root, dirs, files in os.walk(scanpath):
        for f in files:
            if f == 'sacatlog.txt':
                cat_paths.append(os.path.join(root, f))
                if verbose:
                    print('Found soil catalog at:', os.path.join(root, f))

    meta = []
    for path in cat_paths:
        with open(path, 'r') as textfile:
            reader = csv.reader(textfile, quotechar='"', delimiter='|')
            row = next(reader)
            cat_dict = {'areasymbol': row[0], 'areaname': row[1], 'saversion': row[2], 'saverest': row[3],
                        'tabularversion': row[4], 'tabularverest': row[5], 'tabnasisexportdate': row[6],
                        'tabcertstatus': row[7], 'tabcertstatusdesc': row[8], 'fgdcmetadata': row[9],
                        'sacatalogkey': row[10], 'path': path}
        meta.append(cat_dict)
    return meta


def rename_dirs(meta_list, verbose=True):
    dir_altered = 0
    new_meta = list()
    for m in meta_list:
        n = copy.deepcopy(m)
        root = os.path.dirname(os.path.dirname(os.path.dirname(m['path'])))
        dir = os.path.basename(os.path.dirname(os.path.dirname(m['path'])))
        # tabdir = os.path.basename(os.path.dirname(m['path']))
        meta_text = '_v'.join((m['areasymbol'], m['saversion']))
        old_dir = os.path.join(root, dir)
        new_dir = os.path.join(root, meta_text)
        if dir != meta_text:
            if verbose:
                print('Renaming', old_dir, 'to', new_dir)
            try:
                os.rename(old_dir, new_dir)
            except:
                print('Error renaming', old_dir, 'to', new_dir)
                n['old_path'] = None
            else:
                dir_altered += 1
                n['old_path'] = n['path']
                n['path'] = n['path'].replace(old_dir, new_dir)

        else:
            n['old_path'] = None
        new_meta.append(n)
    return dir_altered, new_meta


def rename_files(meta_list, verbose=True):
    f_altered = 0
    for m in meta_list:
        base_dir = os.path.dirname(os.path.dirname(m['path']))
        meta_text = '_v'.join((m['areasymbol'], m['saversion']))
        for root, dirs, files in os.walk(base_dir, topdown=True):
            for f in files:
                ext = os.path.splitext(f)[1]
                base_find = re.findall(r"^(soildb_[A-Z]{2}_\d{4}).*", os.path.splitext(f)[0])
                if base_find:
                    base = base_find[0]
                else:
                    base = None
                if ext in ['.mdb', '.accdb'] and base is not None:
                    f_new = ''.join((base, '_', meta_text, ext))
                    if verbose:
                        print('Renaming', os.path.join(root, f), 'to', os.path.join(root, f_new))
                    try:
                        os.rename(os.path.join(root, f), os.path.join(root, f_new))
                    except:
                        print('Error renaming', os.path.join(root, f), 'to', os.path.join(root, f_new))
                    else:
                        f_altered += 1
            break  # prevents looking into deeper subdirs

    return f_altered


def do_upgrade(new_meta, old_meta, upgrade_path, archive_path=None):
    dirs_moved = 0
    for m in new_meta:
        new_base_dir = os.path.dirname(os.path.dirname(m['path']))
        for o in old_meta:
            old_base_dir = os.path.dirname(os.path.dirname(o['path']))
            if m['areasymbol'] == o['areasymbol'] and int(m['saversion']) > int(o['saversion']):
                print('Replacing', old_base_dir, 'with', new_base_dir)
                if archive_path is not None:
                    print('\tMoving', old_base_dir, 'to', archive_path)
                    shutil.move(old_base_dir, archive_path)
                else:
                    print('\tDeleting', old_base_dir)
                    shutil.rmtree(old_base_dir)
                print('\tMoving', new_base_dir, 'to', upgrade_path)
                shutil.move(new_base_dir, upgrade_path)
                dirs_moved += 1
    return dirs_moved


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder path for folder containing tabular '
                                                 'soil data and rename the parent folder according to the Soil Area '
                                                 'metadata.')
    # positional arguments
    parser.add_argument('scanpath', help='path to recursively scan for downloaded SSURGO soil folders.')
    parser.add_argument('-a', '--rename_access',  action='store_true',
                        help='A flag telling the script to rename the MS Access files with meta info.')
    parser.add_argument('-d', '--rename_dirs',  action='store_true',
                        help='A flag telling the script to rename the soil survey base directory with meta info.')
    parser.add_argument('-u', '--upgrade_path',
                        help='A path other than scanpath which will be searched for soil databases. If DBs are found '
                             'with a lower Soil Area Version, they will be either archived or deleted based on '
                             '--archive option.')
    parser.add_argument('-A', '--archive_path',
                        help='A path other than scanpath to which older versions of a soil surveys will be moved to '
                             'if a newer version is found in scanpath and the --upgrade_path option is chosen. If no '
                             'archive_path is provided, older soil surveys will be deleted.')
    args = parser.parse_args()

    # check for valid arguments
    if not os.path.isdir(args.scanpath):
        print(args.scanpath, "does not exist. Please choose an existing path to search.")
        quit()
    if args.upgrade_path is None and args.rename_dirs is None and args.rename_access is None:
        print('At least one option [rename_dirs, rename_access, upgrade_path] is required.')
    if args.upgrade_path is not None:
        if not os.path.isdir(args.upgrade_path):
            print(args.upgrade_path, "does not exist. Please choose an existing path to upgrade.")
            quit()

    print('Scanning for soil catalog data at', args.scanpath)
    meta = get_meta(scanpath=args.scanpath)
    if meta:
        if args.rename_dirs:
            rd, meta = rename_dirs(meta_list=meta)
            print('Renamed', rd, 'directories.')
        if args.rename_access:
            rf = rename_files(meta_list=meta)
            print('Renamed', rf, 'files.')
        if args.upgrade_path is not None:
            print('Scanning for soil catalog data at', args.upgrade_path)
            old_meta = get_meta(scanpath=args.upgrade_path)
            print('Doing upgrade...')
            moved = do_upgrade(new_meta=meta, old_meta=old_meta, upgrade_path=args.upgrade_path,
                               archive_path=args.archive_path)
            print('Upgraded', moved, 'soil surveys.')
    else:
        print('No soil metadata found.')
    print('Script finished.')
