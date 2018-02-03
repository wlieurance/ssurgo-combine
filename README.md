# Introduction #
The purpose of this toolset is to give the user the ability to quickly combine individual soil area based SSURGOs downloaded from NRCS. These are packaged as a collection of tabular (.txt) and shapefile data for individual Soil Survey Areas.  This toolset combines all downloaded SSURGOs located within the same base folder and imports them into a new SpatiaLite database, usable in ArcMap (>= 10.2), QGIS, R, et al.

This toolset also adds some custom SQL queries and features to the dataset.  See the Custom Additions Section for more info.

# Prerequisites/Installation #
This toolset was written in python 3.6 and does not attempt to be backwards compatible with earlier python version. That being said it will likely function properly for most python3 installations.

This toolset does not require any non-standard python libraries, but **does** require that the modular SpatiaLite library (4.3+) be installed on your system and in the case of MS Windows, its location added manually into either your user or system PATH variable. See the [Gaia-SINS](https://www.gaia-gis.it/gaia-sins/) site for more info on how to install the SpatiaLite modular library for your specific system. 

# Use #
Basic use would be to run the following, modifying python calls as necessary for your system: 

python extract.py "/path/to/scan"

A more complex usage example would be:

python extract.py --dbpath .\test.sqlite --ecosite --groups .\ecogroups.csv --snap 1 --restrict .\surveylist.csv --repair C:\Users\someuser\Documents\SSURGO

Please note that any argument passed to the script containing a space needs to be enclosed in double quotes (e.g. "C:\Soil Surveys"). See the help documentation provided via --help option for more info. 

Depending on if the dbpath the user supplied already exists, this script will create a new SpatiaLite database and import the SSURGO data into it. If the db path exists, his script can be rerun on the existing database in order to import more data.  Any existing data will be skipped in the new import process.

Also note that snapping grid size should be used carefully. It can be useful to fix linear artifacts produced from joining soil surveys that don't quite meet up exactly, but also tends to produce invalid geometries that can't be repaired if the snapping size is too big.  A snap size of less than 2.0 meters is recommended.  Also, if using the snapping option, it is also recommended to use the --repair flag, which can fix some invalid geometries present in the original data (or produced via the ST_SnapToGrid() SpatiaLite function) via the ST_MakeValid() SpatiaLite function.

# Custom Additions #
There are a number of custom SQL views added to provide quick access to certain types of data within the database (e.g. dominant component, surface texture, etc.)  For a look at what each does, there is a brief comment for each located in the create_views.py file. 

Additionally, the database can be populated with a polygon feature called 'ecopolygon' using the -e or --ecosite flags. This is a calculated feature that shows the dominant ecological sites for the imported data (dissolved/aggregated by dominant ecosite within map units) and the area of the final polygon they make up.

Furthermore, for those users wanting custom ecological site groupings in their database, the -g or --group flag can be used with extract.py, or the following command can be run:

python create_ecogroups.py "/path/to/db.sqlite" "/path/to/csvfile.csv"

Run the above with the --help option for more information on the command's specific functionality.  This will create a similar feature to 'ecopolygon' called 'ecogrouppolygon' showing the dominant ecological groups in the imported data and their final percentage of the resulting polygon (dissolved/aggregated by dominant group within map units).  An example csv file containing example ecological groups has been provided. Please note that this format needs to be adhered to strictly or the import will fail. All fields can be filled with blank strings (i.e. '') with the exception of the ecoid and ecogroup fields, which are required. Mainly, it is necessary to make sure one is using a stripped version of the ecosite name (e.g. 027XY001NV) so as to match the 'coecoclassid_std' field from the coecoclass_mudominant view.

# Errors & Warnings #
Users creating a new database will encounter the 'DiscardGeometryColumn() error: not existing Table or Column' error when creating a new database.  This is normal and does not affect script function. These errors will disappear if importing new data into an already existing database.