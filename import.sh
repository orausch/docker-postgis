#! /bin/sh


osm2pgsql -d gis -C 2000 -U docker map.osm.pbf --hstore	-E 21781
