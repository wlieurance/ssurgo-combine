CREATE TABLE IF NOT EXISTS {schema}.ecogroup_meta (
       ecogroup VARCHAR (20) PRIMARY KEY, 
       groupname TEXT, 
       grouptype VARCHAR (50), 
       modal_site VARCHAR (20), 
       pub_status VARCHAR (20), 
       veg_type TEXT);

CREATE TABLE IF NOT EXISTS {schema}.ecogroup (
       ecoid VARCHAR (20) PRIMARY KEY, 
       ecogroup VARCHAR (50) NOT NULL, 
       FOREIGN KEY(ecogroup) REFERENCES {schema}.ecogroup_meta(ecogroup));


DROP TABLE IF EXISTS {schema}.ecogrouppolygon;
/* Creates a new table in which store spatial query results for dominant ecogroup polygons. */
CREATE TABLE IF NOT EXISTS {schema}.ecogrouppolygon (
       ecogroup VARCHAR (50) PRIMARY KEY,
       groupname TEXT,
       grouptype VARCHAR (50),
       pub_status VARCHAR (20),
       area_ha DOUBLE PRECISION,
       ecogrouppct DOUBLE PRECISION);

ALTER TABLE {schema}.ecogrouppolygon ADD COLUMN IF NOT EXISTS geom geometry('MULTIPOLYGON', 4326);