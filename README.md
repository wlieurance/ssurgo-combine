# Introduction #
The purpose of this toolset is to give the user the ability to quickly combine individual soil area based SSURGOs downloaded from NRCS. These are packaged as a collection of tabular (.txt) and shapefile data for individual Soil Survey Areas. This toolset combines all downloaded SSURGOs located within the same base folder and imports them into a new SpatiaLite or PostGIS database, usable in ArcMap/Pro, QGIS, R, et al.

This toolset also adds some custom SQL queries and features to the dataset. See the Custom Additions Section for more info.

# Prerequisites/Installation #
This toolset was written in python 3.8 and does not attempt to be backwards compatible with earlier python version. That being said it will likely function properly for most python3 installations.

This toolset requires some non-standard libraries (see requirements.txt), and also requires that the modular SpatiaLite library (4.3+) or PostGIS (2.4+) be installed on your system. In the case of SpatiaLite on MS Windows, its location needs to be added manually into either your user, or system PATH variable. See the [Gaia-SINS](https://www.gaia-gis.it/gaia-sins/) site for more info on how to install the SpatiaLite modular library for your specific system. 

# Use #
Basic use would be to run the following, modifying python calls as necessary for your system (e.g python vs python3.6 vs py -3.6 etc).: 

`python import_soil.py "/path/to/scan"`

A more complex usage example (SpatiaLite/Windows) for would be:

`python import_soil.py --dbname test.sqlite --ecosite --groups ecogroups_meta.csv ecogroups.csv --snap 1 --restrict surveylist.csv --repair C:\Users\someuser\Downloads\my_scanpath`

An alternative example with PostGIS and short command line options on a \*nix system:

`python import_soil.py --type postgis --dbname mydbname --user myusername --ecosite --schema soil "/home/user/soils folder/my_scanpath"`

Another example using an AWS server for with a postgres instance:

`python import_soil.py --type postgis --dbname mydbname --user myusername --host myinstance.id.locale.rds.amazonaws.com --myport 5432 --ecosite "/home/user/soils folder/my_scanpath"`

Please note that any argument passed to the script containing a space needs to be enclosed in double quotes (e.g. "C:\Soil Surveys"). See the help documentation provided via --help option for more info. For database connections that require a password, if one is not provided with the --password argument, the user will be prompted for the password.

Depending on if the dbname the user supplied already exists, this script will create a new SpatiaLite database and import the SSURGO data into it. If the db path exists, his script can be rerun on the existing database in order to import more data. Any existing data will be skipped during the new import process and newer versions will replace older versions if found. In the case of a PostGIS import, the script will initialize the blank PostgreSQL database as a PostGIS database.

Also note that snapping grid size should be used carefully. It can be useful to fix linear artifacts produced from joining soil surveys that don't quite meet up exactly, but also tends to produce invalid geometries that can't be repaired if the snapping size is too big. A snap size of less than 2.0 meters is recommended. Also, if using the snapping option, it is also recommended to use the --repair flag, which can fix some invalid geometries present in the original data (or produced via the ST_SnapToGrid function) via the ST_MakeValid function.

# Custom non-SSURGO tables and views #
There are a number of custom SQL views added to provide quick access to certain types of data within the database (e.g. dominant component, surface texture, etc.)  For a look at what each does, there is a brief comment for each located in the create_views.sql file. 

Additionally, the database can be populated with a polygon feature called 'ecopolygon' using the -e or --ecosite flags. This is a calculated feature that shows the dominant ecological sites for the imported data (dissolved/aggregated by dominant ecosite within map units) and the area of the final polygon they make up.

Furthermore, for those users wanting custom ecological site groupings in their database, the -g or --group flag can be used with import_soil.py, or the following command can be run:

`python create_ecogroups.py --dbname mydb.sqlite /path/to/ecogroups_meta.csv /path/to/ecogroups.csv`

Run the above with the --help option for more information on the command's specific functionality. This will create a similar feature to 'ecopolygon' called 'ecogrouppolygon' showing the dominant ecological groups in the imported data and their final percentage of the resulting polygon (dissolved/aggregated by dominant group within map units). An example delimited file containing example ecological groups has been provided in the example sub folder. Please note that this format needs to be adhered to strictly or the import will fail. All fields can be filled with blank strings (i.e. '') with the exception of the ecoid and ecogroup fields, which are required. Mainly, it is necessary to make sure one is using a stripped version of the ecosite name (e.g. 027XY001NV) so as to match the 'coecoclassid_std' field from the coecoclass_mudominant view.

# Tools #
There are additional tools available for working with NRCS soil downloads that don't directly apply to database creation and storage.

1. /tools/access_reports.py: This script can be use to export certain soil reports in PDF form from an NRCS SSURGO template MS Access database which has been populated. Run with --help for more information.
2. /tools/populate_access.py: This script can be used to bulk load multiple NRCS SSURGO template databases with tabular and spatial data. Run with --help for more information.
3. /tools/rename_update.py: This script can be used to upgrade soil surveys folders containing spatial and tabular data in a directory with newer soil surveys downloaded from NRCS. The script will either delete or move the older version and replace with the newer version. Run with --help for more information.
4. /tools/report_keys.py: This script can be used to get the report keys for a specific report in a NRCS SSURGO template database. Useful as a precursor to running access_reports.py. Run with --help for more information.
5. /tools/versions.py: This script will scan a directory and export a pipe delimited file containing information about found soil survey folders. Useful for generating a list of soil surveys and their versions before downloading newer versions from NRCS. Run with --help for more information.

# Errors & Warnings #
Users creating a new database may encounter the 'DiscardGeometryColumn() error: not existing Table or Column' warning when creating a new SpatiaLite database. This is normal and does not affect script function. These errors will disappear if importing new data into an already existing database.

Additionally, a user's sqlite3 library distributed with python may not have been compiled with RTree support, which is necessary for spatial indices. A user will see warnings about this if they use a non-Rtree compiled sqlite3 library and spatial indices will be removed by the script. While spatial indices are not strictly necessary, they may dramatically speed up the drawing of some spatial features. Users are encouraged to download a different version from the [SQLite](https://sqlite.org/download.html) project directly and replace the sqlite3 library in your python install directory (C:\Program Files\Python3X\DLLs\sqlite3.dll" in Windows). For \*nix systems which rely on a default python installation, it is recommended to use a virtual environment, or a tool such as pyenv or pipenv instead. Users with a conda installation my already have Rtree compiled, but may have to create a new conda environment for the project.