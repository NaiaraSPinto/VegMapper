#!/usr/bin/env python

import argparse
import warnings

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon


def get_utm_zone(lat, lon):
    utm_zone = int((np.floor((lon + 180) / 6) % 60) + 1)
    if lat >= 0:
        utm_epsg = 32600 + utm_zone
    else:
        utm_epsg = 32700 + utm_zone
    return utm_zone, utm_epsg


def prep_tiles(aoi_name, aoi_boundary, t_size, centered=True,
               x_offset=0, y_offset=0):
    gdf_boundary = gpd.read_file(aoi_boundary)

    # Determine UTM zone
    lat_ctr = np.mean(gdf_boundary.total_bounds[1::2])
    lon_ctr = np.mean(gdf_boundary.total_bounds[0::2])
    _, utm_epsg = get_utm_zone(lat_ctr, lon_ctr)

    # Get UTM tiles
    gdf_boundary_utm = gdf_boundary.to_crs(f'epsg:{utm_epsg}')
    bounds = gdf_boundary_utm.total_bounds

    if centered:
        s = 100         # the tile corner coordinates will be multiples of s
        x_ctr = np.mean(bounds[0::2])
        y_ctr = np.mean(bounds[1::2])
        t_xnum = np.ceil((bounds[2] - bounds[0]) / t_size)
        t_ynum = np.ceil((bounds[3] - bounds[1]) / t_size)
        t_xmin = np.floor((x_ctr - (t_xnum/2)*t_size) / s) * s + x_offset
        t_ymin = np.floor((y_ctr - (t_ynum/2)*t_size) / s) * s + y_offset
        t_xmax = t_xmin + t_xnum * t_size
        t_ymax = t_ymin + t_ynum * t_size
    else:
        t_xmin = np.floor(bounds[0] / t_size) * t_size
        t_ymin = np.floor(bounds[1] / t_size) * t_size
        t_xmax = np.ceil(bounds[2] / t_size) * t_size
        t_ymax = np.ceil(bounds[3] / t_size) * t_size
    t_xs = np.arange(t_xmin, t_xmax, t_size)
    t_ys = np.arange(t_ymax, t_ymin, -t_size)

    boundary_polygon = gdf_boundary_utm.dissolve().geometry[0]

    hs = []
    vs = []
    mask = []
    polygons = []
    for h, x in enumerate(t_xs):
        for v, y in enumerate(t_ys):
            xs = [x, x+t_size, x+t_size, x, x]
            ys = [y, y, y-t_size, y-t_size, y]
            p = Polygon(zip(xs, ys))
            m = boundary_polygon.intersects(p)
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

    out_dir = aoi_boundary.rstrip(aoi_boundary.split('/')[-1]).rstrip('/')
    out_tiles = f'{out_dir}/{aoi_name}_tiles.geojson'
    with warnings.catch_warnings():
        # Ignore the FutureWarning, which will be fixed in the next release of geopandas
        warnings.simplefilter("ignore")
        gdf_tiles.to_file(out_tiles, driver='GeoJSON')
    print(f'Tiles for {aoi_name} is saved to: {out_tiles}')
    print(f"{gdf_tiles['mask'].sum()} out of {len(gdf_tiles['mask'])} tiles "
          f'intersecting {aoi_name}')

    return out_tiles


def main():
    parser = argparse.ArgumentParser(
        description='Prepare UTM tiles covering area of interest (AOI)'
    )
    parser.add_argument('aoi_name', type=str,
                        help='Name of AOI')
    parser.add_argument('aoi_boundary', type=str,
                        help='Boundary of AOI (shp/geojson)')
    parser.add_argument('tile_size', type=int,
                        help='Tile size in meters')
    args = parser.parse_args()

    prep_tiles(args.aoi_name, args.aoi_boundary, args.tile_size)


if __name__ == '__main__':
    main()
