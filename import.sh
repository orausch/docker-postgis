#! /bin/sh
alias SQL="psql --dbname gis -U docker -c"

echo " --- IMPORTING ACCIDENTS DATA ---"
SQL "CREATE TABLE accidents (id serial NOT NULL, jahr int4,kanton varchar,monat int4,tag int4,stunde int4,x int4, y int4, accident int4 CONSTRAINT accidents_pkey PRIMARY KEY (id));"
SQL "COPY accidents(jahr, kanton, monat, tag, stunde, x, y, accident) FROM '/var/lib/postgresql/data/accidents.csv' DELIMITER ',' CSV HEADER;"
SQL "SELECT addgeometrycolumn('accidents', 'way', 21781, 'POINT', 2);"
SQL "UPDATE accidents SET way=ST_SETSRID(ST_MAKEPOINT(x, y), 21781);"
SQL "CREATE INDEX accidents_gix ON accidents USING GIST (way);"
SQL "VACUUM analyse accidents;"
SQL "CLUSTER accidents using accidents_gix;"
SQL "VACUUM analyse accidents;"

echo " --- DONE IMPORTING ACCIDENTS DATA --- "

echo " --- IMPORTING BELASTUNG DATA --- "
shp2pgsql -s 21781 -I /var/lib/postgresql/data/belastung.shp | psql -U docker -d gis -q
SQL "ALTER TABLE belastung ADD COLUMN l_speed int4;"
SQL "ALTER TABLE belastung ADD COLUMN r_speed int4;"
SQL "ALTER TABLE belastung ADD COLUMN speed int4;"
SQL "ALTER TABLE belastung ADD COLUMN lanes int4;"
SQL "ALTER TABLE belastung ADD COLUMN cars int4;"
SQL "ALTER TABLE belastung ADD COLUMN trucks int4;"
SQL "ALTER TABLE belastung ADD COLUMN capacity int4;"
SQL "UPDATE belastung SET lanes = numlanes + r_numlanes;"
SQL 'UPDATE belastung set cars = "volveh_t~5" + "r_volve~11";UPDATE belastung set trucks = "volvehpr~1" + "r_volveh~7" - cars;'
SQL "ALTER TABLE belastung ADD COLUMN usage float;"
SQL 'UPDATE belastung set capacity = capprt + r_capprt;'
SQL 'Update belastung set "usage" = (trucks+cars)::float/capacity::float;'

SQL "UPDATE belastung set r_speed = to_number(btrim(r_v0prt, 'km/h'), '99999');"
SQL "UPDATE belastung set l_speed = to_number(btrim(v0prt, 'km/h'), '99999');"
SQL "UPDATE belastung set speed = (r_speed + l_speed)/ ((r_speed!=0)::int + (l_speed!=0)::int);"
echo " --- DONE IMPORTING BELASTUNG DATA --- "

echo " --- IMPORTING OSM DATA --- "
osm2pgsql -d gis -C 2000 -U docker map.osm.pbf --hstore	-E 21781
# we could remove hte following lines if we filterd out what we want using a
# osm2pgsql config file, but this is easier (but slower)
SQL "DELETE FROM planet_osm_line where highway is null or highway ='steps' or highway='pedestrian' or highway='path' or highway = 'pedestrian', or highway='footway' or highway='service';"
SQL "DELETE FROM planet_osm_point where highway != 'traffic_signals' and highway != 'traffic_signals;crossing' and highway!='crossing;traffic_signals' and highway !='junction';"
SQL "DELETE FROM planet_osm_polygon where building is NULL;"
SQL "VACUUM analyse planet_osm_line;"
SQL "CLUSTER planet_osm_line using planet_osm_line_index;"
SQL "VACUUM analyse planet_osm_line;"
SQL "VACUUM analyse planet_osm_polygon;"
SQL "VACUUM analyse planet_osm_point;"
echo " --- DONE IMPORTING OSM DATA --- "


echo " --- PROCESSING ACCIDENTS DATA --- "
SQL "ALTER TABLE accidents ADD COLUMN surface text[]; ALTER TABLE accidents ADD COLUMN tracktype text[];"
SQL "UPDATE accidents SET surface=subquery.surface, tracktype=subquery.ttype FROM (SELECT acc.id, array_agg(roads.surface) as surface, array_agg(roads.tracktype) as ttype FROM (SELECT id, way FROM accidents) as acc LEFT JOIN (SELECT way, surface, tracktype FROM planet_osm_line) as roads ON st_dwithin(acc.way, roads.way, 500) GROUP BY acc.id) as subquery where accidents.id=subquery.id;"
SQL 'ALTER TABLE accidents add column "surface_asphalt" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_concrete" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_cobble" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_dirt" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_grass" BOOLEAN  NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_gravel" BOOLEAN NOT NULL DEFAULT FALSE; ALTER TABLE accidents add column "surface_paved" BOOLEAN NOT NULL DEFAULT FALSE;'
SQL "UPDATE accidents set surface_asphalt = TRUE where 'asphalt' = ANY(surface) or 'asphalt;concrete' = ANY(surface)or 'asphalt;paved' = ANY(surface); UPDATE accidents set surface_cobble = TRUE where 'cobblestone' = ANY(surface) OR 'cobblestone:flattened' = ANY(surface) or 'paved;cobblestone' = ANY(surface); UPDATE accidents set surface_concrete = TRUE where 'concrete' = ANY(surface) OR 'concrete:lanes' = ANY(surface) or 'concrete:plates' = ANY(surface)  or 'concrete;asphalt' = ANY(surface); UPDATE accidents set surface_dirt = TRUE where 'dirt' = ANY(surface) OR 'grade4' = ANY(tracktype) or 'earth' = ANY(surface)  or 'ground' = ANY(surface)or 'soil' = ANY(surface) or 'sand' = ANY(surface);UPDATE accidents set surface_grass = TRUE where 'grass' = ANY(surface) OR 'grass_paver' = ANY(surface) or 'grade5' = ANY(tracktype); UPDATE accidents set surface_gravel = TRUE where 'grade2' = ANY(tracktype) OR 'grade3' = ANY(tracktype) OR 'fine_gravel' = ANY(surface) OR 'gravel' = ANY(surface) or 'gravel;grass' = ANY(surface)  or 'gravel;ground' = ANY(surface)or 'grit' = ANY(surface) or 'pebblestone' = ANY(surface) OR 'compacted' = ANY(surface); UPDATE accidents set surface_paved = TRUE where 'paved' = ANY(surface) OR 'paving_stones' = ANY(surface) or 'sett' = ANY(surface)  or 'wood' = ANY(surface);"
SQL "ALTER TABLE accidents ADD COLUMN dist_TL float;"
SQL "UPDATE accidents set dist_TL=subquery.distance FROM (SELECT accidents.id, accidents.way, st_distance(accidents.way, points.way) as distance from accidents Cross join lateral (select way from planet_osm_point where highway != 'junction' order by accidents.way <-> way limit 1) as points) as subquery where accidents.id = subquery.id;"

# This is very slow and probably not that intersting, so we omit it
# SQL "ALTER TABLE accidents ADD COLUMN dist_Junction float;"
# SQL "UPDATE accidents set dist_Junction=subquery.distance FROM (SELECT accidents.id, accidents.way, st_distance(accidents.way, points.way) as distance from accidents Cross join lateral (select way from planet_osm_point where highway = 'junction' order by accidents.way <-> way limit 1) as points) as subquery where accidents.id = subquery.id;"
SQL 'ALTER TABLE accidents add column lanes int4, add column speed int4, add column capacity int4, add column trucks int4, add column cars int4, add column "usage" float;'
SQL 'UPDATE accidents set lanes = subquery.lanes, speed=subquery.speed, trucks=subquery.trucks, cars=subquery.cars, capacity=subquery.capacity, "usage"=subquery."usage" FROM (SELECT DISTINCT ON (a.id) a.id, b.lanes, b.speed, b.trucks, b.cars, b.capacity, b."usage" FROM accidents a LEFT JOIN belastung b ON ST_DWITHIN(b.geom, a.way, 100) ORDER BY a.id, st_distance(a.way, b.geom)) as subquery WHERE accidents.id = subquery.id;'
echo " --- DONE PROCESSING ACCIDENTS DATA --- "

