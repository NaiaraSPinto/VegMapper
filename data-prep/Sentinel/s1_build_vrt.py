#!/usr/bin/env python

import argparse
import subprocess
from subprocess import CalledProcessError
from datetime import datetime
from pathlib import Path
import sys
import re

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
    if args.srcpath:
        if args.srcpath[0:5] == 's3://':
            try:
                dst = 's3'
                s3_path = Path(args.srcpath[5:])
                s3_bucket = str(Path(s3_path.parts[0]))
                s3_prefix = str(Path(*s3_path.parts[1:]))
                print(f'Listing s3://{s3_bucket}')
                subprocess.check_call(f'gsutil ls s3://{s3_bucket}', shell=True)
            except:
                raise Exception("Connection to S3 failed. Use 'aws configure' to configure.")
        elif args.srcpath[0:5] == 'gs://':
            try:
                dst = 'gs'
                gs_path = Path(args.srcpath[5:])
                gs_bucket = Path(gs_path.parts[0])
                gs_prefix = Path(*gs_path.parts[1:])
                print(f'Listing gs://{gs_bucket}')
                subprocess.check_call(f'gsutil ls gs://{gs_bucket}', shell=True)
            except:
                raise Exception("Listing gs://{gs_bucket} failed. Use 'gsutil config' to configure.")
        elif Path(args.srcpath).exists():
            dst = 'local'
            local_path = Path(args.srcpath)
        else:
            raise Exception(f'Destination path {args.srcpath} does not exist')

    # These .zip files should be under srcpath/year/path_frame
    year, path_frame = args.year_path_frame.split('_', 1)
    try:
        zip_list = subprocess.check_output(f'gsutil ls {args.srcpath}/{year}/{path_frame}/*.zip', shell=True).decode(sys.stdout.encoding).splitlines()
    except CalledProcessError as e:
        # command matched no files
        print(f"No files were found under {args.srcpath}/{year}/{path_frame}. Ensure that the srcpath and year_path_frame provided were correct.")
        sys.exit()
    regex = re.compile(r"""
                        (?P<dst>s3://|gs://)?                                                       # match the cloud bucket prefix, if it's there (s3:// or gs://)
                        (?P<path>.*[/\\])?                                                          # match any folder names before the granule name
                        (?P<granule>\w{3}_\w{2}_\d{8}T\d{6}_\w{3}_RTC\d{2}_\w{1}_\w{6}_\w{4}.zip)   # match the granule name
                        """, re.VERBOSE)

    gdal_links = []
    for f in zip_list:
        m = regex.match(f)
        if m:
            path = m.group('path')
            granule = m.group('granule')
            if dst == 's3':
                link = f"/vsizip/vsis3/{path}{granule}"
            elif dst == 'gs':
                link = f"/vsizip/vsigs/{path}{granule}"
            elif dst == 'local':
                link = f"vsizip/{path}{granule}"
            gdal_links.append(link)
    
    # TODO: Build VRT using gdalbuildvrt
    # For S3, simply use /vsis3/s3_path/year/path_frame/vrt_name as dstfile for gdalbuildvrt. boto3 not needed.
    # For GCS, I got "Fetching OAuth2 access code from auth code failed" error,
    # let's save VRT locally and upload to GCS using gsutil


if __name__ == '__main__':
    main()
