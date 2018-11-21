#! /bin/sh
alias SQL="psql --dbname gis -U docker -c"

echo "IMPORTING ACCIDENTS DATA"
SQL "CREATE TABLE accidents (id serial NOT NULL, jahr int4,kanton varchar,monat int4,tag int4,stunde int4,x int4, y int4, CONSTRAINT accidents_pkey PRIMARY KEY (id));"
SQL "COPY accidents(jahr, kanton, monat, tag, stunde, x, y) FROM '/accidents.csv' DELIMITER ',' CSV HEADER;"
echo "DONE IMPORTING ACCIDENTS DATA"

echo "IMPORTING OSM DATA"
osm2pgsql -d gis -C 2000 -U docker map.osm.pbf --hstore	-E 21781
echo "DONE IMPORTING OSM DATA"

echo "START PROCESSING ACCIDENTS DATA"
SQL "SELECT addgeometrycolumn('accidents', 'way', 21781, 'POINT', 2);"
SQL "UPDATE accidents SET way=ST_SETSRID(ST_MAKEPOINT(x, y), 21781);"
SQL "SELECT accidents.id, array_agg(roads.surface), array_agg(planet_osm_line.tracktype) FROM accidents LEFT JOIN (SELECT * FROM planet_osm_line WHERE highway is not null and highway !='steps' and highway!='pedestrian' and highway!='path' and highway != 'pedestrian') as roads ON st_dwithin(accidents.way, roads.way, 50) GROUP BY accidents.id;"



SQL 'ALTER TABLE accidents add column "surface_asphalt" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_concrete" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_cobble" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_dirt" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_grass" BOOLEAN  NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_gravel" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_paved" BOOLEAN NOT NULL DEFAULT FALSE;'

SQL "UPDATE accidents set surface_asphalt = TRUE where 'asphalt' = ANY(surface) or 'asphalt;concrete' = ANY(surface)or 'asphalt;paved' = ANY(surface); UPDATE accidents set surface_cobble = TRUE where 'cobblestone' = ANY(surface) OR 'cobblestone:flattened' = ANY(surface) or 'paved;cobblestone' = ANY(surface); UPDATE accidents set surface_concrete = TRUE where 'concrete' = ANY(surface) OR 'concrete:lanes' = ANY(surface) or 'concrete:plates' = ANY(surface)  or 'concrete;asphalt' = ANY(surface); UPDATE accidents set surface_dirt = TRUE where 'dirt' = ANY(surface) OR 'grade4' = ANY(tracktype) or 'earth' = ANY(surface)  or 'ground' = ANY(surface)or 'soil' = ANY(surface) or 'sand' = ANY(surface);UPDATE accidents set surface_grass = TRUE where 'grass' = ANY(surface) OR 'grass_paver' = ANY(surface) or 'grade5' = ANY(tracktype); UPDATE accidents set surface_gravel = TRUE where 'grade2' = ANY(tracktype) OR 'grade3' = ANY(tracktype) OR 'fine_gravel' = ANY(surface) OR 'gravel' = ANY(surface) or 'gravel;grass' = ANY(surface)  or 'gravel;ground' = ANY(surface)or 'grit' = ANY(surface) or 'pebblestone' = ANY(surface) OR 'compacted' = ANY(surface); UPDATE accidents set surface_paved = TRUE where 'paved' = ANY(surface) OR 'paving_stones' = ANY(surface) or 'sett' = ANY(surface)  or 'wood' = ANY(surface);"
