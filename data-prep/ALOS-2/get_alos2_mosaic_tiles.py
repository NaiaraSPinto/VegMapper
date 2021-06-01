import argparse

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('state', help='state name')
parser.add_argument('-h', '--help', action='help', help='get ALOS-2 Mosaic tiles')
args = parser.parse_args()

state = args.state.lower()

shp = f'../AOI/{state}/{state}_boundary.geojson'
gdf = gpd.read_file(shp)

t_size = 1
t_xmin = int(np.floor(gdf.bounds.minx[0]))
t_ymin = int(np.floor(gdf.bounds.miny[0]))
t_xmax = int(np.ceil(gdf.bounds.maxx[0]))
t_ymax = int(np.ceil(gdf.bounds.maxy[0]))

state_boundary = gdf.geometry[0]

tiles = []
polygons = []
for y in range(t_ymax, t_ymin, -t_size):
    for x in range(t_xmin, t_xmax):
        xs = [x, x+t_size, x+t_size, x, x]
        ys = [y, y, y-t_size, y-t_size, y]
        p = Polygon(zip(xs, ys))
        if y >= 0:
            ns = 'N'
        else:
            ns = 'S'
        if x >= 0:
            ew = 'E'
        else:
            ew = 'W'
        tile = f'{ns}{abs(y):02}{ew}{abs(x):03}'
        if state_boundary.intersects(p):
            tiles.append(tile)
            polygons.append(p)
            print(tile)
gdf2 = gpd.GeoDataFrame({'tile': tiles,
                         },
                        crs='epsg:4326',
                        geometry=polygons)
gdf2.to_file(f'{state}_alos2_mosaic_tiles.geojson', driver='GeoJSON')
