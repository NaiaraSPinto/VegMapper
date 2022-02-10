#!/usr/bin/env python

import argparse
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse


pf_pattern = re.compile(r'^\d+_\d+$', re.ASCII)


def s1_proc(storage, proj_dir, year, m1, m2, path_frame=None):
    # Get list of path_frame
    path_frame_list = []
    if path_frame is not None:
        if pf_pattern.fullmatch(path_frame):
            path_frame_list.append(path_frame)
        else:
            raise Exception(f'{path_frame} is not of correct path_frame format.')
    else:
        if storage in ['s3', 'gs']:
            ls_cmd = f'gsutil ls {proj_dir}/sentinel_1/{year}'
            obj_list = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
            for obj_path in obj_list:
                obj_name = Path(obj_path).name
                if obj_path[-1] == '/' and pf_pattern.fullmatch(obj_name):
                    path_frame_list.append(obj_name)
        elif storage == 'local':
            for p in (proj_dir / f'sentinel_1/{year}').iterdir():
                if pf_pattern.match(p.name):
                    path_frame_list.append(p.name)

    for path_frame in path_frame_list:
        print(f'\nProcessing {year}_{path_frame} ...')

        for layer in ['VV', 'VH', 'INC', 'LS']:
            # Build VRT
            subprocess.check_call(f's1_build_vrt.py {proj_dir}/sentinel_1 {year}_{path_frame} {layer} --m1 {m1} --m2 {m2}', shell=True)
            # Calculate temporal mean
            vrtpath = f'{proj_dir}/sentinel_1/{year}/{path_frame}/{year}_{path_frame}_{layer}.vrt'
            subprocess.check_call(f'calc_vrt_stats.py {vrtpath} mean', shell=True)

        # Remove left and right edge pixels
        subprocess.check_call(f's1_remove_edges.py {proj_dir}/sentinel_1/{year}/{path_frame}', shell=True)

        print(f'\nDone processing {year}_{path_frame}.')


def main():
    parser = argparse.ArgumentParser(
        description='processing Sentinel-1 RTC data'
    )
    parser.add_argument('proj_dir', metavar='proj_dir',
                        type=str,
                        help=('project directory (s3:// or gs:// or local dirs); '
                              'Sentinel-1 processed granules (.zip) are expected '
                              'to be found under proj_dir/sentinel_1/year/'))
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

    # Check proj_dir
    u = urlparse(args.proj_dir)
    if u.scheme == 's3' or u.scheme == 'gs':
        storage = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        proj_dir = f'{storage}://{bucket}/{prefix}'
        subprocess.check_call(f'gsutil ls {storage}://{bucket}',
                              stdout=subprocess.DEVNULL,
                              shell=True)
    else:
        storage = 'local'
        proj_dir = Path(args.proj_dir)
        if not proj_dir.is_dir():
            raise Exception(f'{proj_dir} is not a valid directory path')

    print(f'\nProcessing Sentinel-1 RTC data in {proj_dir}/sentinel_1/{args.year} ...')

    s1_proc(storage, proj_dir, args.year, args.m1, args.m2, args.path_frame)

    print(f'\nDONE processing Sentinel-1 RTC data.')


if __name__ == '__main__':
    main()