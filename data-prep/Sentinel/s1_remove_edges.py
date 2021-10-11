#!/usr/bin/env python

import argparse
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import rasterio


def s1_remove_edges(srcloc, path_frame_dir, year, path_frame, edge_depth):
    print(f'\nRemoving 200 edge pixels on the left and right ... ')
    vv = f'{path_frame_dir}/{year}_{path_frame}_VV_mean.tif'
    vv_edge_removed = Path(f'{year}_{path_frame}_VV_mean.tif')
    ls = f'{path_frame_dir}/{year}_{path_frame}_LS_mean.tif'
    subprocess.check_call((f'remove_edges.py {vv} {vv_edge_removed} '
                           f'--maskfile {ls} '
                           f'--lr_only --edge_depth {edge_depth}'),
                           shell=True)
    with rasterio.open(vv_edge_removed) as dset:
        mask = dset.read_masks(1)
    for layer in ['VV', 'VH', 'INC']:
        tif = f'{path_frame_dir}/{year}_{path_frame}_{layer}_mean.tif'
        tif_edge_removed = Path(f'{year}_{path_frame}_{layer}_mean.tif')
        if layer != 'VV':
            with rasterio.open(tif) as dset:
                data = dset.read(1)
                if layer == 'INC':
                    data = np.rad2deg(data)
                data[mask == 0] = dset.nodata
                profile = dset.profile
            with rasterio.open(tif_edge_removed, 'w', **profile) as dset:
                dset.write(data, 1)
        if srcloc in ['s3', 'gs']:
            subprocess.check_call(f'gsutil cp {tif_edge_removed} {tif}', shell=True)
            Path(tif_edge_removed).unlink()
        elif srcloc == 'local':
            shutil.copyfile(tif_edge_removed, tif)
            tif_edge_removed.unlink()


def main():
    parser = argparse.ArgumentParser(
        description='remove left/right edges (cross-track direction) of Sentinel-1 RTC product'
    )
    parser.add_argument('path_frame_dir', metavar='path_frame_dir',
                        type=str,
                        help=('directory of path-frame '
                              '(s3|gs://bucket/prefix/year/path_frame or local_path/year/path_frame)'))
    parser.add_argument('--edge_depth', metavar='edge_depth', dest='edge_depth',
                        type=int,
                        default=200,
                        help=('edge depth to be removed'))
    args = parser.parse_args()

    # Check and identify path_frame_dir
    u = urlparse(args.path_frame_dir)
    if u.scheme == 's3' or u.scheme == 'gs':
        srcloc = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        year = Path(u.path).parent.name
        path_frame = Path(u.path).name
        path_frame_dir = f'{srcloc}://{bucket}/{prefix}'
        subprocess.check_call(f'gsutil ls {srcloc}://{bucket}/{prefix}',
                              stdout=subprocess.DEVNULL,
                              shell=True)
    else:
        srcloc = 'local'
        path_frame_dir = Path(u.path)
        year = path_frame_dir.parent.name
        path_frame = path_frame_dir.name
        if not path_frame_dir.exists():
            raise Exception(f'Source path {args.srcpath} does not exist')

    # Check if year and path_frame are valid
    if not re.fullmatch(r'[1-2][0-9][0-9][0-9]', year):
        raise Exception(f'{year} is not a valid year.')
    if not re.fullmatch(r'\d+_\d+', path_frame):
        raise Exception(f'{path_frame} is not a valid path_frame.')

    s1_remove_edges(srcloc, path_frame_dir, year, path_frame, args.edge_depth)


if __name__ == '__main__':
    main()