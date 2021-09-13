#!/usr/bin/env python

import argparse
import subprocess
from subprocess import CalledProcessError
from datetime import datetime
from pathlib import Path
import sys
import re
import os
import time

# map layer passed as argument to corresponding tif suffix
layer_map = {
    'VV': 'VV',
    'VH': 'VH',
    'INC': 'inc_map',
    'LS': 'ls_map'}
"""
Given the name of a .zip file, generates the full link of the corresponding
.tif file inside the .zip file that we want to build the VRT with in a format
that GDAL understands.
'dst' should either be 's3', 'gs', or 'local'
'polarization' should either be 'VV', 'VH', 'inc_map', or 'ls_map'

"""
def generate_tif_link(filename, dst, layer):    
    regex = re.compile(r"""
                       (?P<dst>s3://|gs://)?    # match the cloud bucket prefix, if it's there (s3:// or gs://)
                       (?P<path>.*[/\\])?       # match any folder names before the granule name 
                       # match the granule name 
                       (?P<granule>\w{3}_\w{2}_\d{8}T\d{6}_\w{3}_RTC\d{2}_\w{1}_\w{6}_\w{4}(?=.zip))
                       """, re.VERBOSE)

    # 'vsizip' tells GDAL that we are working with a .zip file
    # 'vsis3' tells GDAL that the file is hosted in an S3 bucket
    # 'vsigs' tells GDAL that the file is hosted in a GCS bucket
    m = regex.match(filename)
    if m:
        path = m.group('path')
        if not path:
            path = ""
        granule = m.group('granule')
        if dst == 's3':
            link = f"/vsizip/vsis3/{path}{granule}"
        elif dst == 'gs':
            link = f"/vsizip/vsigs/{path}{granule}"
        elif dst == 'local':
            link = f"vsizip/{path}{granule}"

    # build full filename, including name of tif files inside zip archive that we want
    link = f"{link}.zip/{m.granule}/{m.granule}_{layer}.tif"
    return link

def build_vrt_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame):

    VV_VRT_filename, VH_VRT_filename, INC_VRT_filename = build_vrt(s3, dest_bucket, prefix_str, year, path_frame)

    VV_VRT_s3_key = os.path.join(prefix_str, year, path_frame, VV_VRT_filename)
    VH_VRT_s3_key = os.path.join(prefix_str, year, path_frame, VH_VRT_filename)
    INC_VRT_s3_key = os.path.join(prefix_str, year, path_frame, INC_VRT_filename)

    upload_to_s3(s3, VV_VRT_filename, dest_bucket, VV_VRT_s3_key)
    upload_to_s3(s3, VH_VRT_filename, dest_bucket, VH_VRT_s3_key)
    upload_to_s3(s3, INC_VRT_filename, dest_bucket, INC_VRT_s3_key)

    return VV_VRT_filename, VH_VRT_filename, INC_VRT_filename

"""
Blocks the thread until we can confirm that filename exists.
Used to prevent a race condition in build_vrt.
"""
def block_until_file_created(filename, max_attempts=5):
    attempts = 0

    while attempts < max_attempts:
        if os.path.isfile(filename):
            # exit the function
            return None
        attempts += 1
        time.sleep(5)

    # if we exceed max_attempts and file still not found
    # TODO: Raise FileNotFound Error
    raise RuntimeError("{} not found".format(filename))
    
def build_vrt(s3, bucket_str, prefix_str, year, path_frame):
    bucket_object = s3.Bucket(bucket_str)

    VV_list = []
    VH_list = []
    INC_list = []

    folder = os.path.join(prefix_str, year, path_frame)
    for file in bucket_object.objects.filter(Prefix=folder):
        if file.key.endswith('.zip'):
            VV_list.append(generate_tif_full_filename(bucket_str, file.key, "VV"))
            VH_list.append(generate_tif_full_filename(bucket_str, file.key, "VH"))
            INC_list.append(generate_tif_full_filename(bucket_str, file.key, "inc_map"))

    # TODO: any other kwargs we're missing? targetAlignedPixels?
    VV_VRT_filename = f"{year}_{path_frame}_VV.vrt"
    gdal_build_vrt(VV_VRT_filename, VV_list)

    VH_VRT_filename = f"{year}_{path_frame}_VH.vrt"
    gdal_build_vrt(VH_VRT_filename, VH_list)

    INC_VRT_filename = f"{year}_{path_frame}_INC.vrt"
    gdal_build_vrt(INC_VRT_filename, INC_list)

    # prevent race condition where we attempt to upload the files to s3 before gdal creates the vrt
    block_until_file_created(VV_VRT_filename)
    block_until_file_created(VH_VRT_filename)
    block_until_file_created(INC_VRT_filename)

    # returns the keys to the VV and VRT files in S3
    return VV_VRT_filename, VH_VRT_filename, INC_VRT_filename

def gdal_build_vrt(filename, tif_list):
    cmd = (f'gdalbuildvrt -overwrite '
          f'{filename} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)
        
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

    
    # TODO: Build VRT using gdalbuildvrt
    # For S3, simply use /vsis3/s3_path/year/path_frame/vrt_name as dstfile for gdalbuildvrt. boto3 not needed.
    # For GCS, I got "Fetching OAuth2 access code from auth code failed" error,
    # let's save VRT locally and upload to GCS using gsutil


if __name__ == '__main__':
    main()
