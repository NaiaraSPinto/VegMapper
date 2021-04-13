from hyp3_sdk import HyP3
from osgeo import gdal
from configparser import ConfigParser
import pandas as pd
from datetime import datetime,timezone

import boto3
from botocore.exceptions import ClientError

import os
import sys
import logging
import concurrent.futures
import ntpath
import time
import subprocess

import re

from copy import copy
from pathlib import Path
import numpy as np

# TODO: Use global variables for things like s3, bucket_name?

def main():
    logging.basicConfig(filename='error.log', encoding='utf-8', level=logging.DEBUG)
    if len(sys.argv) != 2:
        print("error: missing config file or too many arguments")
        print("USAGE: python automation.py <config file>")
    else:
        config_file = sys.argv[1]
    config = ConfigParser()
    config.read(config_file)
    
    hyp3_username = config['HyP3']['username']
    hyp3_password = config['HyP3']['password']

    aws_access_key_id = config['aws']['aws_access_key_id']
    aws_secret_access_key = config['aws']['aws_secret_access_key']
    dest_bucket = config['aws']['dest_bucket']
    prefix_str = config['aws']['prefix_str']

    submit_granules_switch = config['switches']['submit_granules']
    copy_processed_granules_to_bucket = config['switches']['copy_processed_granules_to_bucket']
    calculate_temporal_average = config['switches']['calculate_temporal_average']
    
    max_threads = int(config['misc']['max_threads'])
    
    hyp3 = HyP3(username=hyp3_username, password=hyp3_password)
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    granules_group_dict = generate_granules_group_dict(config['csv']['csv'])
    
    if submit_granules_switch.lower() == "t" or submit_granules_switch.lower() == "true":
        submit_granules(hyp3, granules_group_dict)
    
    # use threading to prevent entire script crashing if one operation throws an error
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for year_path_frame in granules_group_dict.keys():
            year, path_frame = year_path_frame.split('_', 1)
            futures.append(executor.submit(thread_function, hyp3, s3, dest_bucket, prefix_str, year, path_frame, copy_processed_granules_to_bucket, calculate_temporal_average))

        for fut in futures:
            print(fut.result())
        executor.shutdown(wait=True) # wait for all threads to finish

    print("done with everything")

def thread_function(hyp3, s3, dest_bucket, prefix_str, year, path_frame, copy_processed_granules_to_bucket, calculate_temporal_average):
    if copy_processed_granules_to_bucket.lower() == "t" or copy_processed_granules_to_bucket.lower() == "true":
        print(year, path_frame, "copying jobs to bucket")
        copy_granules_to_bucket(hyp3, s3, dest_bucket, prefix_str, year, path_frame)
        print(year, path_frame, "DONE copying jobs to bucket")

    if calculate_temporal_average.lower() == "t" or calculate_temporal_average.lower() == "true":
        print(year, path_frame, "building vrt and uploading to s3")
        VV_VRT_filename, VH_VRT_filename = build_vrt_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame)
        print(year, path_frame, "DONE building vrt and uploading to s3")

        print(year, path_frame, "calc temp avg and uploading to s3")
        calc_temp_avg_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame, VV_VRT_filename, VH_VRT_filename)
        print(year, path_frame, "DONE calc temp avg and uploading to s3")

"""
Returns the granules dictionary
"""
def generate_granules_group_dict(csv_path):
    granules_df = pd.read_csv(csv_path)
    granules_df['Year'] = granules_df['Acquisition Date'].apply(lambda x: x.split('-')[0])
    granules_df = granules_df.filter(['Granule Name','Year','Path Number','Frame Number'])
    granules_df['year_path_frame'] = granules_df.apply(lambda row: "{}_{}_{}".format(row['Year'], row['Path Number'], row['Frame Number']), axis=1)

    granules_groups = granules_df.groupby(by=['year_path_frame'])['Granule Name']

    # create a dictionary
    # key example: 'year_path_frame' --> '2018_25_621'
    # value is a list of granule names that have the year,path,frame in the key
    granules_group_dict = {
        key : granules_groups.get_group(x).to_list()
        for key,x in zip(granules_groups.indices,granules_groups.groups)
    }

    return granules_group_dict

"""
Submits the granules to Hyp3.  
"""
def submit_granules(hyp3, granules_group_dict):
    print("You will be submitting the following granules for processing:")
    for year_path_frame,granule_name_list in granules_group_dict.items():
        print("{} - {} granules".format(year_path_frame, len(granule_name_list)))
    user_input = input("Enter 'Y' to confirm you would like to submit these granules: ")
    if user_input.lower() != "y":
        sys.exit()

    # submit the jobs for all year_path_frame's
    for year_path_frame,granule_name_list in granules_group_dict.items():
        for granule_name in granule_name_list:
                hyp3.submit_rtc_job(granule_name, year_path_frame, resolution=30.0, radiometry='gamma0',
                                    scale='power', speckle_filter=False, dem_matching=True,
                                    include_dem=True, include_inc_map=True, include_scattering_area=True)

    print("Jobs successfully submitted.")

def copy_granules_to_bucket(hyp3, s3, dest_bucket, prefix_str, year, path_frame):
    year_path_frame = "_".join([year, path_frame])

    batch = hyp3.find_jobs(name=year_path_frame) 
    batch = hyp3.watch(batch)
    
    for job in batch.jobs:
        copy_source = {
            'Bucket':job.files[0]['s3']['bucket'],
            'Key':job.files[0]['s3']['key']
        }
        filename = ntpath.basename(job.files[0]['s3']['key'])
        destination_key = os.path.join(prefix_str, '{}/{}/{}'.format(year, path_frame, filename))
        expiration_time = job.expiration_time
        today = datetime.now(timezone.utc)
        if not today > expiration_time: # TODO: Today's date in utc
            try:
                s3.meta.client.copy(copy_source, Bucket=dest_bucket, Key=destination_key) 
            except:
                logging.error("Error copying from {} to {}/{}".format(copy_source,dest_bucket,destination_key))
                
        else:
            # job is expired and cannot be copied
            pass

def build_vrt_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame):

    VV_VRT_filename, VH_VRT_filename = build_vrt(s3, dest_bucket, prefix_str, year, path_frame)

    VV_VRT_s3_key = os.path.join(prefix_str, year, path_frame, VV_VRT_filename)
    VH_VRT_s3_key = os.path.join(prefix_str, year, path_frame, VH_VRT_filename)

    upload_to_s3(s3, VV_VRT_filename, dest_bucket, VV_VRT_s3_key)
    upload_to_s3(s3, VH_VRT_filename, dest_bucket, VH_VRT_s3_key)

    return VV_VRT_filename, VH_VRT_filename

def build_vrt(s3, bucket_str, prefix_str, year, path_frame):
    # if the bucket has more files that we don't care about, use this to filter only files that start with the given prefix string
    bucket_object = s3.Bucket(bucket_str)

    VV_list = []
    VH_list = []
    
    folder = os.path.join(prefix_str, year, path_frame)
    for file in bucket_object.objects.filter(Prefix=folder):
        if file.key.endswith('.zip'):
            VV_list.append(generate_tif_full_filename(bucket_str, file.key, "VV"))
            VH_list.append(generate_tif_full_filename(bucket_str, file.key, "VH"))

    # TODO: any other kwargs we're missing? targetAlignedPixels?
    VV_VRT_filename = "{}_{}_VV.vrt".format(year, path_frame)
    gdal_build_vrt(VV_VRT_filename, VV_list)

    VH_VRT_filename = "{}_{}_VH.vrt".format(year, path_frame)
    gdal_build_vrt(VH_VRT_filename, VH_list)
    
    # prevent race condition where we attempt to upload the files to s3 before gdal creates the vrt
    block_until_file_created(VV_VRT_filename)
    block_until_file_created(VH_VRT_filename)

    # returns the keys to the VV and VRT files in S3
    return VV_VRT_filename, VH_VRT_filename

"""
Given the name of a .zip file in an AWS bucket, generates the full filename
of the corresponding .tif file inside the .zip file that we want to build the
VRT with in a format that GDAL understands.
Polarization should either be "VV" or "VH"
"""
def generate_tif_full_filename(bucket, filename, polarization):
    # TODO: Make this a parameter. 
    # Hardcoded variable controls whether we are building links for S3 or local filesystem
    cloud = True
    local_path = "/data3/lepopal_vault/datasets/2018_data"

    regex = re.compile(r"""
                    (.*[/\\])?   # matches any folder names before the granule name
                    (.+)         # matches the granule name
                    (?:\.zip)    # matches the .zip file extension
                    """, re.VERBOSE)

    match = re.search(regex, filename)
    if not match:
        raise ValueError("unrecognized filename format: " + filename)

    folder = match.group(1)
    if not folder:
        folder = ""
    granule_name = match.group(2)

    if cloud:
        # build the full filename of the .tif file we want
        # 'vsizip' tells GDAL that we are working with a .zip file
        # 'vsis3' tells GDAL that the file is hosted in an s3 bucket
        # return ("/vsizip/vsis3/" + bucket + "/" + folder + granule_name + ".zip/" + granule_name
        #         + "/" + granule_name + "_" + polarization + ".tif")
        return os.path.join(
            "/vsizip/vsis3/", bucket, folder, granule_name + ".zip/", granule_name, 
            granule_name + "_" + polarization + ".tif"
        )
    else:
        return os.path.join(
            "/vsizip/" + local_path, granule_name + ".zip/", granule_name, 
            granule_name + "_" + polarization + ".tif"
        )

def gdal_build_vrt(filename, tif_list):
    # TODO: Move these static variables to global?
    pixfun_name = 'average'
    pixfun = """
import numpy as np
def average(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
    div = np.zeros(in_ar[0].shape)
    for i in range(len(in_ar)):
        div += (in_ar[i] != 0)
    div[div == 0] = 1
    out_ar[:] = np.sum(in_ar, axis=0, dtype='double') / div
"""

    contents = f"""    <PixelFunctionType>{pixfun_name}</PixelFunctionType>
        <PixelFunctionLanguage>Python</PixelFunctionLanguage>
        <PixelFunctionCode><![CDATA[{pixfun}]]>
        </PixelFunctionCode>
    """

    cmd = (f'gdalbuildvrt -overwrite '
          f'{filename} {" ".join(tif_list)}')
    subprocess.check_call(cmd, shell=True)

    with open(filename) as f:
        lines = f.readlines()

    lines[3] = lines[3].replace('band="1"',
                                'band="1" subClass="VRTDerivedRasterBand"')

    lines.insert(4, contents)

    with open(filename, 'w') as f:
        f.writelines(lines)

""" 
Blocks the thread until we can confirm that filename exists.
Used to prevent a race condition in build_vrt.
"""
def block_until_file_created(filename, max_attempts=3):
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

def calc_temp_avg_and_upload_to_s3(s3, dest_bucket, prefix_str, year, path_frame, VV_VRT_filename, VH_VRT_filename):
    print("VV...", end=None)
    vv_tif_filename = calc_temp_avg(VV_VRT_filename)
    vv_tif_s3_key = os.path.join(prefix_str, year, path_frame, vv_tif_filename)
    upload_to_s3(s3, vv_tif_filename, dest_bucket, vv_tif_s3_key)
    print("DONE")

    print("VH...", end=None)
    vh_tif_filename = calc_temp_avg(VH_VRT_filename)
    vh_tif_s3_key = os.path.join(prefix_str, year, path_frame, vh_tif_filename)
    upload_to_s3(s3, vh_tif_filename, dest_bucket, vh_tif_s3_key)
    print("DONE")

    # after uploading VRTs and averaging, delete them from local filesystem
    os.remove(VV_VRT_filename)
    os.remove(VH_VRT_filename)

    # after uploading .tifs, delete them from local filesystem
    os.remove(vv_tif_filename)
    os.remove(vh_tif_filename)

def calc_temp_avg(vrt_filename):
    tif_filename = os.path.splitext(ntpath.basename(vrt_filename))[0] + ".tif"
    cmd = (f'gdal_translate --config '
          f'GDAL_VRT_ENABLE_PYTHON YES '
          f'{vrt_filename} {tif_filename}')
    subprocess.check_call(cmd, shell=True)

    return tif_filename

def upload_to_s3(s3, local_filepath, bucket, key):
    try:
        s3.meta.client.upload_file(Filename=local_filepath, Bucket=bucket, Key=key)
    except ClientError as e:
        # TODO: add descriptive error messages
        print("ERROR!", e)


if __name__ == '__main__':
   main()
