#!/usr/bin/env python

import argparse
import getpass
import subprocess
from datetime import datetime
from math import ceil, floor
from pathlib import Path
from urllib.parse import urlparse

import geopandas as gpd
import numpy as np
import requests
from shapely.geometry import Polygon


def get_tiles(aoi):
    gdf = gpd.read_file(aoi)
    gdf = gdf.to_crs('epsg:4326')
    aoi_polygon = gdf.geometry[0]
    aoi_bounds = aoi_polygon.bounds

    t_size = 1      # Mosaic data are in 1 x 1 degree tiles
    t_xmin = int(np.floor(aoi_bounds[0]))
    t_ymin = int(np.floor(aoi_bounds[1]))
    t_xmax = int(np.ceil(aoi_bounds[2]))
    t_ymax = int(np.ceil(aoi_bounds[3]))

    tile_list = []
    for y in range(t_ymax, t_ymin, -t_size):
        for x in range(t_xmin, t_xmax, t_size):
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
            if aoi_polygon.intersects(p):
                tile_list.append(tile)
    print(f'\nTiles to be downloaded: ', *tile_list)
    return tile_list


def download_tiles(tile_list, year, dst, jaxa_username, jaxa_password):
    for i, tile in enumerate(tile_list):
        if year < 2014:
            # ALOS
            file = f'{tile}_{str(year)[2:]}_MOS.tar.gz'
        else:
            # ALOS-2
            file = f'{tile}_{str(year)[2:]}_MOS_F02DAR.tar.gz'

        print(f'\nDownloading {i+1} / {len(tile_list)} tiles ...')

        ns = tile[0]    # N or S
        ew = tile[3]    # E or W
        lat_tile = int(tile[1:3])   # lat. of upper-left corner of 1-deg tile
        lon_tile = int(tile[4:7])   # lon. of upper-left corner of 1-deg tile

        # lat. of upper-left coner of 5-deg grid
        if ns == 'N':
            lat_grid = ceil(lat_tile/5)*5
        else:
            lat_grid = floor(lat_tile/5)*5
        # lon. of upper-left coner of 5-deg grid
        if ew == 'E':
            lon_grid = floor(lon_tile/5)*5
        else:
            lon_grid = ceil(lon_tile/5)*5
        if lat_grid == 0:
            ns = 'N'
        if lon_grid == 0:
            ew = 'E'
        grid = f'{ns}{lat_grid:02}{ew}{lon_grid:03}'

        url = (f'https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/data/{year}/'
               f'dir_gz/{year}/{grid}/{file}')
        cmd = f'wget --user {jaxa_username} --password {jaxa_password} '
        if isinstance(dst, Path):
            cmd = cmd + f'-c -P {dst} {url}'
            subprocess.call(cmd, shell=True)
        elif isinstance(dst, str):
            # cmd = cmd + f'-O- {url} | gsutil cp - {dst}/{file}'
            cmd = cmd + f'-c -P . {url}'
            subprocess.call(cmd, shell=True)
            cmd = f'gsutil cp {file} {dst}/{file}'
            subprocess.call(cmd, shell=True)
            Path(file).unlink()


def main():
    parser = argparse.ArgumentParser(
        description='download ALOS/ALOS-2 Mosaic data from JAXA website'
    )
    parser.add_argument('proj_dir', metavar='proj_dir',
                        type=str,
                        help=('project directory (s3:// or gs:// or local dirs); '
                              'ALOS/ALOS-2 mosaic data (.tar.gz) will be stored '
                              'under proj_dir/alos2_mosaic/year/tarfiles/'))
    parser.add_argument('aoi', metavar='aoi',
                        type=str,
                        help='shp/geojson of area of interest (AOI)')
    parser.add_argument('year', metavar='year',
                        type=int,
                        help=('year'))
    args = parser.parse_args()

    # Get JAXA login info
    print(f'Downloading ALOS/ALOS-2 Mosaic data requires a JAXA account, '
          f'which can be registered from: '
          f'https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/registration.htm')
    jaxa_username = input('\nEnter JAXA Username: ')
    jaxa_password = getpass.getpass('Enter JAXA Password: ')

    # Get a list of years where ALOS/ALOS-2 data are available
    available_years = [2007, 2008, 2009, 2010, 2015, 2016, 2017, 2018, 2019]
    for yr in range(2020, datetime.now().year+1):
        r = requests.get(f'https://www.eorc.jaxa.jp/ALOS/en/palsar_fnf/data/{yr}/map.htm',
                         auth=(jaxa_username, jaxa_password))
        if r.status_code == 200:
            available_years.append(yr)
        else:
            break

    # Check year
    if args.year not in available_years:
        raise Exception(f'ALOS/ALOS-2 data is not available for year {args.year}')

    # Check proj_dir
    u = urlparse(args.proj_dir)
    if u.scheme == 's3' or u.scheme == 'gs':
        storage = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        proj_dir = f'{storage}://{bucket}/{prefix}'
        dst_dir = f'{storage}://{bucket}/{prefix}/alos2_mosaic/{args.year}/tarfiles'
        subprocess.check_call(f'gsutil ls {storage}://{bucket}',
                              stdout=subprocess.DEVNULL,
                              shell=True)
    else:
        storage = 'local'
        proj_dir = Path(args.proj_dir)
        if not proj_dir.is_dir():
            raise Exception(f'{proj_dir} is not a valid directory path')
        dst_dir = proj_dir / f'alos2_mosaic/{args.year}/tarfiles'
        if not dst_dir.exists():
            dst_dir.mkdir(parents=True)

    tile_list = get_tiles(args.aoi)
    download_tiles(tile_list, args.year, dst_dir, jaxa_username, jaxa_password)


if __name__ == '__main__':
    main()