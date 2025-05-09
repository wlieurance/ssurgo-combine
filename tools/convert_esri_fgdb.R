library(sf)
library(DBI)
library(RSQLite)
library(glue)
library(tibble)

db_path <- file.path("C:/Users/wlieurance/Documents/MLRA_27_NV/Database",
                     "mlra27_soil.sqlite")
out_dir <- "C:/Users/wlieurance/Documents/MLRA_27_NV/Database"
con <- dbConnect(RSQLite::SQLite(), db_path)
dbExecute(con, "SELECT load_extension('mod_spatialite');")
# cxn.conn.execute("PRAGMA foreign_keys = ON;")

fgdb_path = "C:/Users/wlieurance/Documents/MLRA_27_NV/Database/temp.gdb"

tabular_list <- c(
  "mdstatdommas",
  "mdstattabs",
  "sacatalog",
  "sdvalgorithm",
  "sdvattribute",
  "sdvfolder",
  "featdesc",
  "legend",
  "mdstatdomdet",
  "mdstatidxmas",
  "mdstatrshipmas",
  "mdstattabcols",
  "sdvfolderattribute",
  "distlegendmd",
  "distmd",
  "distinterpmd",
  "laoverlap",
  "legendtext",
  "mapunit",
  "mdstatidxdet",
  "mdstatrshipdet",
  "sainterp",
  "component",
  "muaggatt",
  "muaoverlap",
  "mucropyld",
  "mutext",
  "chorizon",
  "cocanopycover",
  "cocropyld",
  "codiagfeatures",
  "coecoclass",
  "coeplants",
  "coerosionacc",
  "coforprod",
  "cogeomordesc",
  "cohydriccriteria",
  "cointerp",
  "comonth",
  "copmgrp",
  "copwindbreak",
  "corestrictions",
  "cosurffrags",
  "cotaxfmmin",
  "cotaxmoistcl",
  "cotext",
  "cotreestomng",
  "cotxfmother",
  "chaashto",
  "chconsistence",
  "chdesgnsuffix",
  "chfrags",
  "chpores",
  "chstructgrp",
  "chtext",
  "chtexturegrp",
  "chunified",
  "coforprodo",
  "copm",
  "cosoilmoist",
  "cosoiltemp",
  "cosurfmorphgc",
  "cosurfmorphhpp",
  "cosurfmorphmr",
  "cosurfmorphss",
  "chstruct",
  "chtexture",
  "chtexturemod",
  "featline", 
  "featpoint", 
  "muline", 
  "mupoint", 
  "mupolygon", 
  "sapolygon",
  "ecopolygon",
  "ecogrouppolygon",
  "ecogroup",
  "ecogroup_meta",
  "coecoclass_area",
  "coecoclass_codominant",
  "coecoclass_mapunit_ranked",
  "coecoclass_mudominant",
  "coecoclass_plantprod",
  "coecoclass_unique",
  "coecoclass_wide",
  "coeplants_gh",
  "component_dominant",
  "component_mapunit_ranked",
  "component_plantprod",
  "component_unique",
  "component_wide",
  "ecogroup_area",
  "ecogroup_detail",
  "ecogroup_mapunit_ranked",
  "ecogroup_mudominant",
  # "ecogroup_plantprod",
  "ecogroup_unique",
  "ecogroup_wide"
)

tbls_sql <- "SELECT name FROM sqlite_master WHERE type IN ('table', 'view');"
sqlite_tbls <- dbGetQuery(con, tbls_sql)

start <- 79
for (i in start:length(tabular_list)) {
  tbl <- tabular_list[i]
  if (tbl %in% sqlite_tbls$name) {
    print(paste0("Importing ", tbl, "..."))
    # sql <- glue("SELECT * FROM {tbl};")
    df <- st_read(dsn = db_path, layer = tbl, as_tibble = TRUE, 
                  drivers = "SQLite", quiet = TRUE)
    if (nrow(df) > 0) {
      st_write(obj = df, dsn = fgdb_path, layer = tbl, driver = "OpenFileGDB", 
               append = FALSE, quiet = TRUE)
    } else {
      print(paste(tbl, "empty. Skipping..."))
    }
  } else {
    print(paste(tbl, "not in database. Skipping..."))
  }
}
