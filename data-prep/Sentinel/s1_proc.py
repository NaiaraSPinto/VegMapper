#!/usr/bin/env python

import argparse
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import rasterio

pf_pattern = re.compile(r'^\d+_\d+$', re.ASCII)


def s1_proc(srcloc, srcpath, year, m1, m2, path_frame=None):
    # Get list of path_frame
    path_frame_list = []
    if path_frame is not None:
        if pf_pattern.match(path_frame):
            path_frame_list.append(path_frame)
        else:
            raise Exception(f'{path_frame} is not of correct path_frame format.')
    else:
        if srcloc == 's3' or srcloc == 'gs':
            ls_cmd = f'gsutil ls {srcpath}/{year}'
            obj_list = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
            for obj_path in obj_list:
                obj_name = Path(obj_path).name
                if obj_path[-1] == '/' and pf_pattern.match(obj_name):
                    path_frame_list.append(obj_name)
        elif srcloc == 'local':
            for p in (srcpath/year).iterdir():
                if pf_pattern.match(p.name):
                    path_frame_list.append(p.name)

    for path_frame in path_frame_list:
        print(f'\nProcessing {year}_{path_frame} ...')

        for layer in ['VV', 'VH', 'INC', 'LS']:
            # Build VRT
            subprocess.check_call(f's1_build_vrt.py {srcpath} {year}_{path_frame} {layer} --m1 {m1} --m2 {m2}', shell=True)
            # Calculate temporal mean
            vrtpath = f'{srcpath}/{year}/{path_frame}/{year}_{path_frame}_{layer}.vrt'
            subprocess.check_call(f'calc_vrt_stats.py {vrtpath} mean', shell=True)

        # Remove left and right edge pixels
        print(f'\nRemoving 200 edge pixels on the left and right ... ')
        if srcloc == 's3' or srcloc == 'gs':
            srcpath = srcpath.replace(f'{srcloc}://', f'/vsi{srcloc}/')
        vv = f'{srcpath}/{year}/{path_frame}/{year}_{path_frame}_VV_mean.tif'
        vv_ex = f'{srcpath}/{year}/{path_frame}/{year}_{path_frame}_VV_mean_ex.tif'
        ls = f'{srcpath}/{year}/{path_frame}/{year}_{path_frame}_LS_mean.tif'
        subprocess.check_call((f'remove_edges.py {vv} {vv_ex} '
                               f'--maskfile {ls} '
                               f'--lr_only --edge_depth 200'),
                               shell=True)
        with rasterio.open(vv_ex) as dset:
            mask = dset.read_masks(1)
        for layer in ['VH', 'INC']:
            tif = f'{srcpath}/{year}/{path_frame}/{year}_{path_frame}_{layer}_mean.tif'
            with rasterio.open(tif, 'r+') as dset:
                data = dset.read(1)
                data[mask == 0] = dset.nodata
                dset.write(data, 1)
        print(f'Done processing {year}_{path_frame}.')


def main():
    parser = argparse.ArgumentParser(
        description='processing Sentinel-1 RTC data'
    )
    parser.add_argument('srcpath', metavar='srcpath',
                        type=str,
                        help=('source path to where processed granules are stored '
                              '(AWS S3 - s3://srcpath, GCS - gs://srcpath, local storage - srcpath)'))
    parser.add_argument('year', metavar='year',
                        type=str,
                        help='year of granules to be processed')
    parser.add_argument('--pf', metavar='path_frame', dest='path_frame',
                        type=str,
                        help=('path_frame of granules to be processed'))
    parser.add_argument('--m1', metavar='m1', dest='m1',
                        type=int,
                        default=1,
                        help=('granules with acquisition month >= m1 will be processed'))
    parser.add_argument('--m2', metavar='m2', dest='m2',
                        type=int,
                        default=12,
                        help=('granules with acquisition month <= m2 will be processed'))
    args = parser.parse_args()

    # Check and identify srcpath
    u = urlparse(args.srcpath)
    if u.scheme == 's3' or u.scheme == 'gs':
        srcloc = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        srcpath = f'{srcloc}://{bucket}/{prefix}'
        print(f'Listing {srcloc}://{bucket}/{prefix}')
        subprocess.check_call(f'gsutil ls {srcloc}://{bucket}/{prefix}', shell=True)
    else:
        srcloc = 'local'
        srcpath = Path(args.srcpath)
        if not srcpath.exists():
            raise Exception(f'Source path {args.srcpath} does not exist')

    s1_proc(srcloc, srcpath, args.year, args.m1, args.m2, args.path_frame)


if __name__ == '__main__':
    main()