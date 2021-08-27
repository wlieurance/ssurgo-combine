-- temporary tables for raw insert before formatting using ST_Multi() and GROUPing
DROP TABLE IF EXISTS {schema}.featline_shp;
CREATE TABLE {schema}.featline_shp (
    OBJECTID SERIAL PRIMARY KEY NOT NULL,
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    featsym VARCHAR (3),
    featkey VARCHAR (30));

DROP TABLE IF EXISTS {schema}.featpoint_shp;
CREATE TABLE {schema}.featpoint_shp (
    OBJECTID SERIAL PRIMARY KEY NOT NULL,
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    featsym VARCHAR (3),
    featkey VARCHAR (30));

DROP TABLE IF EXISTS {schema}.muline_shp;
CREATE TABLE {schema}.muline_shp (
    OBJECTID SERIAL PRIMARY KEY NOT NULL,
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    musym VARCHAR (6),
    mukey VARCHAR (30));

DROP TABLE IF EXISTS {schema}.mupoint_shp;
CREATE TABLE {schema}.mupoint_shp (
    OBJECTID SERIAL PRIMARY KEY NOT NULL,
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    musym VARCHAR (6),
    mukey VARCHAR (30));

DROP TABLE IF EXISTS {schema}.mupolygon_shp;
CREATE TABLE {schema}.mupolygon_shp (
    OBJECTID SERIAL PRIMARY KEY NOT NULL,
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    musym VARCHAR (6),
    mukey VARCHAR (30));

DROP TABLE IF EXISTS {schema}.sapolygon_shp;
CREATE TABLE {schema}.sapolygon_shp (
    OBJECTID SERIAL PRIMARY KEY NOT NULL,
    areasymbol VARCHAR (20),
    spatialver DOUBLE PRECISION,
    lkey VARCHAR (30));


ALTER TABLE {schema}.featline_shp ADD COLUMN IF NOT EXISTS geom geometry(LINESTRING, 4326);
ALTER TABLE {schema}.featpoint_shp ADD COLUMN IF NOT EXISTS geom geometry(POINT, 4326);
ALTER TABLE {schema}.muline_shp ADD COLUMN IF NOT EXISTS geom geometry(LINESTRING, 4326);
ALTER TABLE {schema}.mupoint_shp ADD COLUMN IF NOT EXISTS geom geometry(POINT, 4326);
ALTER TABLE {schema}.mupolygon_shp ADD COLUMN IF NOT EXISTS geom geometry(POLYGON, 4326);
ALTER TABLE {schema}.sapolygon_shp ADD COLUMN IF NOT EXISTS geom geometry(POLYGON, 4326);