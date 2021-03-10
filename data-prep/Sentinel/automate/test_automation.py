from hyp3_sdk import HyP3
from osgeo import gdal
from configparser import ConfigParser
import pandas as pd
import datetime

import boto3
from botocore.exceptions import ClientError

import os
import concurrent.futures
import ntpath
import time

import re

from copy import copy
from pathlib import Path
import numpy as np
import rasterio

# TODO: Use global variables for things like s3, bucket_name?

def main():
    config_file = 'config.ini'
    config = ConfigParser()
    config.read(config_file)
    
    aws_access_key_id = config['aws']['aws_access_key_id']
    aws_secret_access_key = config['aws']['aws_secret_access_key']
    prefix_str = config['aws']['prefix_str']
    dest_bucket = config['aws']['dest_bucket']
    max_threads = int(config['misc']['max_threads'])
    
    hyp3 = HyP3(username=config['HyP3']['username'], password=config['HyP3']['password'])
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    ### read .csv file and group granules by year_path_frame. 
    # TODO: Make this into a function
    granules_df = pd.read_csv(config['csv']['csv'])
    granules_df['Year'] = granules_df['Acquisition Date'].apply(lambda x: x.split('-')[0])
    granules_df = granules_df.filter(['Granule Name','Year','Path Number','Frame Number'])
    granules_df['year_path_frame'] = granules_df.apply(lambda row: "{}_{}_{}".format(row['Year'], row['Path Number'], row['Frame Number']), axis=1)

    granules_groups = granules_df.groupby(by=['year_path_frame'])['Granule Name']

    # create a dictionary
    # key example: 'year_path_frame' --> '2018_25_621'
    # value is a list of granule names that have the year,path,frame in the key
    granules_group_dict = {key:granules_groups.get_group(x).to_list()
                        for key,x in zip(granules_groups.indices,granules_groups.groups)
                        }

    # submit the jobs for all year_path_frame's
    start_date = datetime.datetime.today() # the date the jobs were submitted
    # for year_path_frame,granule_name_list in granules_group_dict.items():
    #     for granule_name in granule_name_list:
    #             hyp3.submit_rtc_job(granule_name, year_path_frame, resolution=30.0, radiometry='gamma0',
    #                                 scale='power', speckle_filter=False, dem_matching=True,
    #                                 include_dem=True, include_inc_map=True, include_scattering_area=True)
    ###

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for year_path_frame in granules_group_dict.keys():
            futures.append(executor.submit(thread_function, hyp3, s3, prefix_str, dest_bucket, year_path_frame, granule_name_list, start_date))

        for fut in futures:
            print(fut.result())
        executor.shutdown(wait=True) # wait for all threads to finish

    print("done with everything")

def thread_function(hyp3, s3, prefix_str, dest_bucket, year_path_frame, granule_name_list, start_date):
    print(year_path_frame)

    batch = hyp3.find_jobs(name=year_path_frame, start=start_date) # only get jobs after today's date
    batch = hyp3.watch(batch)

    print(year_path_frame, "copying jobs to bucket")
    for job in batch.jobs:
        copy_source = {
            'Bucket':job.files[0]['s3']['bucket'],
            'Key':job.files[0]['s3']['key']
        }
        filename = ntpath.basename(job.files[0]['s3']['key'])
        year, path_frame = year_path_frame.split('_', 1)
        destination_key = os.path.join(prefix_str, '{}/{}/{}'.format(year, path_frame, filename))
        s3.meta.client.copy(copy_source, Bucket=dest_bucket, Key=destination_key)

    print(year_path_frame, "DONE copying jobs to bucket")

    print(year_path_frame, "building vrt and uploading to s3")
    VV_VRT_s3_key, VH_VRT_s3_key = build_vrt(s3, dest_bucket, prefix_str, year, path_frame)
    print(year_path_frame, "DONE building vrt and uploading to s3")

    # print(year_path_frame, "calculating temporal avg VV")
    # calc_temp_avg(s3, VV_VRT_s3_key, year, path_frame, dest_bucket, prefix_str, "VV")
    # print(year_path_frame, "calculating temporal avg VH")
    # calc_temp_avg(s3, VH_VRT_s3_key, year, path_frame, dest_bucket, prefix_str, "VH")

    print(year_path_frame, "done with everything")

"""
Given the name of a .zip file in an AWS bucket, generates the full filename
of the corresponding .tif file inside the .zip file that we want to build the
VRT with in a format that GDAL understands.
Polarization should either be "VV" or "VH"
"""
def generate_tif_full_filename(bucket, filename, polarization):
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

    # build the full filename of the .tif file we want
    # 'vsizip' tells GDAL that we are working with a .zip file
    # 'vsis3' tells GDAL that the file is hosted in an s3 bucket
    return ("/vsizip/vsis3/" + bucket + "/" + folder + granule_name + ".zip/" + granule_name
            + "/" + granule_name + "_" + polarization + ".tif")
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
        
    # if we exceed max_attempts and file still not 
    # is this the right error type?
    raise RuntimeError("{} not found".format(filename))
    
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
    VV_VRT = gdal.BuildVRT(VV_VRT_filename, VV_list, separate = True)

    VH_VRT_filename = "{}_{}_VH.vrt".format(year, path_frame)
    VH_VRT = gdal.BuildVRT(VH_VRT_filename, VH_list, separate = True)
    
    # prevent race condition where we attempt to upload the files to s3 before gdal creates the vrt
    block_until_file_created(VV_VRT_filename)
    block_until_file_created(VH_VRT_filename)
            
    VV_VRT_s3_key = os.path.join(prefix_str, year, path_frame, "VV.vrt")
    upload_to_s3(s3, VV_VRT_filename, bucket_str, VV_VRT_s3_key)

    VH_VRT_s3_key = os.path.join(prefix_str, year, path_frame, "VH.vrt")
    upload_to_s3(s3, VH_VRT_filename, bucket_str, VH_VRT_s3_key)

    # after uploading VRTs, delete them from local filesystem
    os.remove(VV_VRT_filename)
    os.remove(VH_VRT_filename)

    # returns the keys to the VV and VRT files in S3
    return VV_VRT_s3_key, VH_VRT_s3_key

def upload_to_s3(s3, local_filepath, bucket, key):
    try:
        s3.meta.client.upload_file(Filename=local_filepath, Bucket=bucket, Key=key)
    except ClientError as e:
        # TODO: add descriptive error messages
        print("ERROR!", e)

"""
in_vrt is the path to a .vrt file on S3
"""
def calc_temp_avg(s3, in_vrt_key, year, path_frame, bucket_str, prefix_str, polarization):
    # the VRT is located in S3 so need to create a string so rasterio can open it
    rasterio_vrt_s3_filepath = os.path.join("s3://", bucket_str, in_vrt_key)
    print("HERE", rasterio_vrt_s3_filepath)
    with rasterio.open(rasterio_vrt_s3_filepath) as vdset:
        width = vdset.width
        height = vdset.height

        # Raster profile for bands in VRT
        out_prof = copy(vdset.profile)
        out_prof.update(driver='GTiff', count=1)

        dsum = np.zeros((height, width))    # data sum
        npix = np.zeros((height, width))    # number of pixels
        ave = np.zeros((height, width))     # temporal average

        for i in range(vdset.count):
            fp = Path(vdset.files[i+1])
            print(i, fp.name)
            data = vdset.read(i+1)
            mask = vdset.read_masks(i+1)
            dsum = dsum + data
            npix = npix + mask/255
            # note that npix won't necessarily be number of rasters in VRT for all pixels

    ave[npix != 0] = dsum[npix != 0] / npix[npix != 0]

    out_tif = '{}_{}_{}_temp_ave.tif'.format(year, path_frame, polarization)
    with rasterio.open(out_tif, 'w', **out_prof) as dset:
        dset.write(np.float32(ave), 1)

    key = os.path.join(prefix_str, year, path_frame, out_tif)
    upload_to_s3(s3, out_tif, bucket_str, key)

    # remove file after it's done uploading to s3
    os.remove(out_tif)

def main2():
    s3 = boto3.resource('s3')
    VV_VRT_s3_key = 'geotiffs/peru/sentinel_1/2018/98_622/VV.vrt'
    year_path_frame = '2018_98_622'
    destination_bucket = 'jpl-process-sentinel-granules-test'
    prefix_str = 'geotiffs/peru/sentinel_1/2018/98_622/'
    calc_temp_avg(s3, VV_VRT_s3_key, year_path_frame, destination_bucket, prefix_str)

if __name__ == '__main__':
   main()
   #main2()
