#!/usr/bin/env bash
# usage: sqlite2fgdb_min.sh src.sqlite dest.gdb

SRC="$1"
DST="$2"

# Query SQLite for tables/views to KEEP (exclude metadata & index tables)
mapfile -t LAYERS < <(
  SQLITE3_NO_READLINE=1 sqlite3 -batch -init /dev/null "$SRC" "
SELECT name
  FROM sqlite_master
 WHERE type IN ('table', 'view')
   AND name NOT LIKE 'idx_%'
   AND name NOT LIKE 'sqlite_%'
   AND name NOT LIKE 'sql_%'
   AND name NOT LIKE 'sqlean_%'
   AND name NOT LIKE 'spatialite_%'
   AND name NOT LIKE 'spatial_%'
   AND name NOT LIKE 'virts_%'
   AND name NOT LIKE 'views_%'
   AND name NOT LIKE 'vector_%'
   AND name NOT LIKE 'data_%'
   AND name NOT LIKE 'geometry_%'
   AND name NOT LIKE 'geom_%'
   AND name NOT IN ('ElementaryGeometries', 'KNN2', 'SpatialIndex')
 ORDER BY name;
")

# Convert to FileGDB
ogr2ogr -overwrite -f OpenFileGDB "$DST" "$SRC" \
  -oo LIST_ALL_TABLES=YES \
  -lco TARGET_ARCGIS_VERSION=ARCGIS_PRO_3_2_OR_LATER \
  -forceNullable \
  -unsetFid \
  "${LAYERS[@]}"
