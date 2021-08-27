import os
import csv
import argparse


def find_versions(path, out):
    files_read = 0
    fields_populated = 0
    with open(out, 'w', newline='') as csvfile:
        fieldnames = ['ssa_symbol', 'ssa_name', 'ssa_version', 'version_estdate', 'spatial_format', 'coordsys']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')
        writer.writeheader()
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.lower() == 'readme.txt':
                    files_read += 1
                    with open(os.path.join(root, file)) as f:
                        ssa_dict = {}
                        for line in f:
                            parts = line.strip().split()
                            if parts:
                                if len(parts) >= 3:
                                    if ' '.join(parts[0:2]).lower() == 'ssa symbol:':
                                        ssa_dict['ssa_symbol'] = ' '.join(parts[2:])
                                        fields_populated += 1
                                    elif ' '.join(parts[0:2]).lower() == 'ssa name:':
                                        ssa_dict['ssa_name'] = ' '.join(parts[2:])
                                        fields_populated += 1
                                    elif ' '.join(parts[0:2]).lower() == 'ssa version:':
                                        ssa_dict['ssa_version'] = ' '.join(parts[2:])
                                        fields_populated += 1
                                    elif ' '.join(parts[0:3]).lower() == 'ssa version est.:':
                                        ssa_dict['version_estdate'] = ' '.join(parts[3:])
                                        fields_populated += 1
                                    elif ' '.join(parts[0:2]).lower() == 'spatial format:':
                                        ssa_dict['spatial_format'] = ' '.join(parts[2:])
                                        fields_populated += 1
                                    elif ' '.join(parts[0:2]).lower() == 'coordinate system:':
                                        ssa_dict['coordsys'] = ' '.join(parts[2:])
                                        fields_populated += 1
                        if ssa_dict:
                            writer.writerow(ssa_dict)
    return files_read, fields_populated


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a directory for SSURGO downloads and export '
                                                 'a text file containing current Soil Survey Area versions found '
                                                 'within.')
    # positional arguments
    parser.add_argument('scanpath',
                        help='path to scan for NRCS SSURGO downloads.')
    parser.add_argument('outfile',
                        help='The path at which to create the output pipe delimited file.')
    args = parser.parse_args()

    files_pop, fields = find_versions(path=args.scanpath, out=args.outfile)
    print('Finished with', files_pop, 'readme.txt files read and', fields, 'fields populated.')
