#!/usr/bin/env python3
import win32com.client
import pywintypes  # for error handling
import os
import argparse

# # for multi-threading
# import pythoncom
# import threading
# import time


# def start():
#     # Initialize
#     pythoncom.CoInitialize()
#
#     # Get instance
#     ac = win32com.client.Dispatch("Access.Application")
#
#     # Create id
#     ac_id = pythoncom.CoMarshalInterThreadInterfaceInStream(pythoncom.IID_IDispatch, ac)
#
#     # Pass the id to the new thread
#     thread = threading.Thread(target=run_in_thread, kwargs={'ac_id': ac_id})
#     thread.start()
#
#     # Wait for child to finish
#     thread.join()
#
#
# def run_in_thread(ac_id):
#     # Initialize
#     pythoncom.CoInitialize()
#
#     # Get instance from the id
#     ac = win32com.client.Dispatch(
#             pythoncom.CoGetInterfaceAndReleaseStream(ac_id, pythoncom.IID_IDispatch)
#     )
#     time.sleep(5)

def import_access(scanpath):
    ac = win32com.client.gencache.EnsureDispatch("Access.Application")
    ac.Visible = False
    imported = 0
    for root, dirs, files in os.walk(scanpath):
        for f in files:
            if os.path.splitext(f)[1] in ['.mdb', '.accdb']:
                file_path = os.path.join(root, f)
                tab_path = os.path.join(root, 'tabular')
                if os.path.isdir(tab_path):
                    # print('opening', file_path)
                    try:
                        ac.OpenCurrentDatabase(file_path)
                    except pywintypes.com_error:
                        ac.CloseCurrentDatabase()
                        ac.Quit()
                        ac = None
                        raise

                    try:
                        empty = ac.Run('LegendTableEmpty')[0]
                    except pywintypes.com_error:
                        print(file_path, "does not appear to be SSURGO template DB. Skipping...")
                    else:
                        if empty:
                            print('Importing tabular data into', file_path)
                            ac.DoCmd.OpenForm('Import')
                            ac.Forms('Import').Controls('pathspecification').Value = tab_path
                            result = ac.Run('Import_Main')
                            imported += 1
                        else:
                            print(file_path, 'appears to already be populated.')
                    ac.CloseCurrentDatabase()
    ac.Quit()
    ac = None
    return imported


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder path for folder containing tabular '
                                                 'soil data and run native MS Access VBA functions to import tabular '
                                                 'data into the provided MS Access database.')
    # positional arguments
    parser.add_argument('scanpath', help='path to recursively scan for downloaded SSURGO soil folders.')
    args = parser.parse_args()

    # check for valid arguments
    if not os.path.isdir(args.scanpath):
        print(args.scanpath, "does not exist. Please choose an existing path to search.")
        quit()

    # # multi-threading
    # start()

    i = import_access(scanpath=args.scanpath)
    print('Imported tabular data into', i, 'soil databases.')
    print('Script finished.')
