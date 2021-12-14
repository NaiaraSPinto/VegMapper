#!/usr/bin/env python

import argparse
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon
from unidecode import unidecode


def get_utm_zone(lat, lon):
    utm_zone = int((np.floor((lon + 180) / 6) % 60) + 1)
    if lat >= 0:
        utm_epsg = 32600 + utm_zone
    else:
        utm_epsg = 32700 + utm_zone
    return utm_zone, utm_epsg


def get_tiles(shp_boundary, t_size, aoi_name):
    gdf_boundary = gpd.read_file(shp_boundary)

    # Determine UTM zone
    lat_ctr = np.mean(gdf_boundary.geometry[0].bounds[1::2])
    lon_ctr = np.mean(gdf_boundary.geometry[0].bounds[0::2])
    utm_zone, utm_epsg = get_utm_zone(lat_ctr, lon_ctr)

    # Get UTM tiles
    gdf_boundary_utm = gdf_boundary.to_crs(f'epsg:{utm_epsg}')
    bounds = gdf_boundary_utm.geometry[0].bounds
    t_xmin = np.floor(bounds[0] / t_size) * t_size
    t_ymin = np.floor(bounds[1] / t_size) * t_size
    t_xmax = np.ceil(bounds[2] / t_size) * t_size
    t_ymax = np.ceil(bounds[3] / t_size) * t_size
    t_xs = np.arange(t_xmin, t_xmax, t_size)
    t_ys = np.arange(t_ymax, t_ymin, -t_size)

    state_boundary = gdf_boundary_utm.geometry[0]

    hs = []
    vs = []
    mask = []
    polygons = []
    for h, x in enumerate(t_xs):
        for v, y in enumerate(t_ys):
            xs = [x, x+t_size, x+t_size, x, x]
            ys = [y, y, y-t_size, y-t_size, y]
            p = Polygon(zip(xs, ys))
            m = state_boundary.intersects(p)
            hs.append(h)
            vs.append(v)
            if m:
                mask.append(1)
            else:
                mask.append(0)
            polygons.append(p)
    gdf_tiles = gpd.GeoDataFrame({'h': hs,
                                'v': vs,
                                'mask': mask},
                                crs=f'epsg:{utm_epsg}',
                                geometry=polygons)
    out_tiles = f'{aoi_name}_tiles.geojson'
    gdf_tiles.to_file(out_tiles, driver='GeoJSON')
    print(f'Tiles for {aoi_name.capitalize()}: {out_tiles}')

    print(f"{gdf_tiles['mask'].sum()} out of {len(gdf_tiles['mask'])} tiles intersecting {aoi_name.capitalize()}")


def main():
    parser = argparse.ArgumentParser(
        description='prepare boundary and tiles for AOI'
    )
    parser.add_argument('aoi_name', metavar='aoi_name',
                        type=str,
                        help='name of area of interest (AOI)')
    parser.add_argument('aoishp',
                       metavar='aoishp',
                       type=Path,
                       help='shp/geojson of AOI')
    parser.add_argument('t_size', metavar='tile_size',
                        type=int,
                        help=('tile size in meters'))
    args = parser.parse_args()

    shp_boundary = args.aoishp

    get_tiles(shp_boundary, args.t_size, args.aoi_name.lower())


if __name__ == '__main__':
    main()