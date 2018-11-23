import subprocess

path = "/var/lib/postgresql/switzerland.tif"
image_path = "/var/lib/postgresql/images/{}.png"
image_path_aux = "/var/lib/postgresql/images/{}.png.aux.xml"
password="docker"
user = "docker"


command_buildings = "gdal_rasterize -burn 255 -burn 0\
    -sql \"SELECT way from planet_osm_polygon where building is not null\"\
    PG:'host=localhost dbname=gis password=docker user=docker' {}\
    -te {} {} {} {} -ts {} {} -ot byte"

command_roads = "gdal_rasterize -b 2\
    -sql \"SELECT way from planet_osm_line\"\
    -burn 255\
    PG:'host=localhost dbname=gis password=docker user=docker' {}"


from osgeo import gdal
from pg import DB
import subprocess
import tqdm
import os


extent = [467500, 64000, 850500, 321000]
# be very careful inputing scales: the difference in extents must be a multiple
# of scale
def create_raster(scale):
    size_x = extent[2]-extent[0]
    size_y = extent[3]-extent[1]
    size_x = size_x/scale
    size_y = size_y/scale

    command_buildings_f = command_buildings.format(path, extent[0], extent[1],
                                                 extent[2], extent[3],
                                                 size_x, size_y)

    command_roads_f = command_roads.format(path)
    subprocess.check_call(command_buildings_f, shell=True)
    subprocess.check_call(command_roads_f, shell=True)
    f = open("{}.meta.txt".format(path), "w")
    f.write("Generated at scale={} using extent={}, size_x={}\
            size_y={}".format(scale, extent, size_x, size_y))


def render(x, y, name, scale, size):
    bounds=scale*size/2;
    ds = gdal.Open(path)
    ds = gdal.Translate(image_path.format(name),
            ds,
            projWin=[x-bounds, y+bounds, x+bounds, y-bounds],
            format="PNG",
            bandList=[1, 2, 2],
            strict=True
            )
    ds = None
    os.remove(image_path_aux.format(name))

def renderall(scale, size):
    db = DB(dbname='gis', host='localhost', port=5432, user=user,
            passwd=password)

    q = db.query("Select id, x, y from accidents")

    for (idx, x, y) in tqdm.tqdm(q.getresult()):
        render(x,y,idx,scale,size)
