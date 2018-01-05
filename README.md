# Introduction #
The purpose of this toolset is to give the user the ability to quickly combine individual soil area based SSURGOs downloaded from NRCS. These are packaged as a collection of tabular (.txt) and shapefile data for individual Soil Survey Areas.  This toolset combines all downloaded SSURGOs located within the same base folder and imports them into a new SpatiaLite database, usable in ArcMap (>= 10.2), QGIS, R, et al.

This toolset also adds some custom SQL queries and features to the dataset.  See the Custom Additions Section for more info.

# Prerequisites/Installation #
This toolset was written in python 3.6 and does not attempt to be backwards compatible with earlier python version. That being said it will likely function properly for most python3 installations.

This toolset does not require any non-standard python libraries, but **does** require that the modular SpatiaLite library (4.3+) be installed on your system and in the case of MS Windows, its location added manually into either your user or system PATH variable. See the [Gaia-SINS](https://www.gaia-gis.it/gaia-sins/) site for more info on how to install the SpatiaLite modular library for your specific system. 

# Use #
Basic use would be to run the following, modifying python calls as necessary for your system: 

python main.py "/path/to/scan"

See the help documentation provided via --help option for more info. This will create a local database in the script directory called SSURGO.sqlite and import any SSURGOs found within the scan path.

Additionally for those users wanting custom ecological site groupings in their database, the following command can be run:

python create_ecogroups.py "/path/to/db.sqlite" "/path/to/csvfile.csv"

Run the above with the --help option for more information on the command's specific functionality.