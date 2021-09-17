#!/usr/bin/env python

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# RTC zip filename pattern
rtc_zip_pattern = re.compile(r'^\w{3}_\w{2}_\d{8}T\d{6}_\w{3}_RTC\d{2}_\w{1}_\w{6}_\w{4}.zip$')

# GeoTIFF suffix of corresponding data layer in the processed granule zip file
layer_suffix = {
    'VV': 'VV',
    'VH': 'VH',
    'INC': 'inc_map',
    'LS': 'ls_map',
}

def main():
    parser = argparse.ArgumentParser(
        description='build a VRT to contain time-series of Sentinel-1 processed granules')
    parser.add_argument('srcpath', metavar='srcpath',
                        type=str,
                        help=('source path to where processed granules are stored'
                              '(AWS S3 - s3://srcpath, GCS - gs://srcpath, local storage - srcpath)'))
    parser.add_argument('year_path_frame', metavar='year_path_frame',
                        type=str,
                        help='Year_Path_Frame of processed granules (e.g., 2020_171_617)')
    parser.add_argument('layer', metavar='layer',
                        type=str,
                        choices=['VV', 'VH', 'INC', 'LS'],
                        help=('data layer of processed granule to be included in VRT'
                              '(VV / VH / INC / LS)'))
    parser.add_argument('--m1', metavar='m1', dest='m1',
                        type=int,
                        default=1,
                        help=('granules with acquisition month >= m1 will be included in VRT'))
    parser.add_argument('--m2', metavar='m2', dest='m2',
                        type=int,
                        default=12,
                        help=('granules with acquisition month <= m2 will be included in VRT'))
    args = parser.parse_args()

    # Check and identify srcpath
    u = urlparse(args.srcpath)
    if u.scheme == 's3' or u.scheme == 'gs':
        src = u.scheme
        bucket = u.netloc
        prefix = u.path.strip('/')
        srcpath = f'{src}://{bucket}/{prefix}'
        print(f'Listing {src}://{bucket}/{prefix}')
        subprocess.check_call(f'gsutil ls {src}://{bucket}/{prefix}', shell=True)
    else:
        src = 'local'
        srcpath = Path(args.srcpath)
        if not srcpath.exists():
            raise Exception(f'Source path {args.srcpath} does not exist')

    layer = layer_suffix[args.layer]

    # The zip files should be under srcpath/year/path_frame
    year, path_frame = args.year_path_frame.split('_', 1)
    ls_cmd = f'ls {srcpath}/{year}/{path_frame}/*.zip'
    if src == 's3' or src == 'gs':
        ls_cmd = 'gsutil ' + ls_cmd
    zip_list = subprocess.check_output(ls_cmd, shell=True).decode(sys.stdout.encoding).splitlines()
    if not zip_list:
        raise Exception(f'No zip files were found under {srcpath}/{year}/{path_frame}. Ensure that the srcpath and year_path_frame provided were correct.')

    tif_list = []
    for zip_path in zip_list:
        zip_name = Path(zip_path).name
        # Check zip files to see if it's really a RTC zip file
        if not rtc_zip_pattern.match(zip_name):
            continue
        acquisition_time = datetime.strptime(zip_name.split('_')[2], '%Y%m%dT%H%M%S')
        # Skip the ones acquired outside [m1, m2]
        if (acquisition_time.month < args.m1) | (acquisition_time.month > args.m2):
            continue
        # Get vsi paths
        granule = Path(zip_path).stem
        if src == 's3' or src == 'gs':
            vsi_path = f'/vsizip/vsi{src}/{bucket}/{prefix}/{year}/{path_frame}/{zip_name}/{granule}/{granule}_{layer}.tif'
        else:
            vsi_path = f'/vsizip/{srcpath}/{year}/{path_frame}/{zip_name}/{granule}/{granule}_{layer}.tif'
        tif_list.append(vsi_path)
    if not tif_list:
        raise Exception(f'No RTC zip files were found under {srcpath}/{year}/{path_frame}. Ensure that the srcpath and year_path_frame provided were correct.')

    if src == 's3':
        vrt = f'/vsis3/{bucket}/{prefix}/{year}/{path_frame}/{year}_{path_frame}_{args.layer}.vrt'
    else:
        vrt = Path(f'{year}_{path_frame}_{args.layer}.vrt')

    # Build VRT
    print(f'Building {args.layer} VRT for year_path_frame {year}_{path_frame} ...')
    cmd = f'gdalbuildvrt -overwrite {vrt} {" ".join(tif_list)}'
    subprocess.check_call(cmd, shell=True)

    if src == 'gs':
        # RHC: when using /vsigs to make gdalbuildvrt directly output VRT, I got
        # "Fetching OAuth2 access code from auth code failed" error. Let's save
        # VRT locally and upload to GCS using gsutil for now.
        subprocess.check_call(f'gsutil cp {vrt} {srcpath}/{vrt}')
        vrt.unlink()
    elif src == 'local':
        vrt.rename(f'{srcpath}/{year}/{path_frame}/{vrt.name}')


if __name__ == '__main__':
    main()
