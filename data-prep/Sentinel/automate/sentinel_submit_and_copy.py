from hyp3_sdk import HyP3

import pandas as pd
import numpy as np
import rasterio

import boto3
from botocore.exceptions import ClientError

import getpass
import argparse
import os
import sys
import traceback
import re
import ntpath
import time
import subprocess
from datetime import datetime,timezone
from copy import copy
from pathlib import Path

# TODO: Use global variables for things like s3, bucket_name?

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--bucket_path', type=str, default='servir-public/geotiffs/peru/sentinel_1', help='Enter S3 bucket path to store processed granules (e.g. servir-public/geotiffs/peru/sentinel_1)')
    parser.add_argument('--csv', type=str, help='Path to CSV file that contains granules to be submitted or copied.', required=True)
    parser.add_argument('-h', '--help', action='help', help='Display help information.')
    args = parser.parse_args()

    hyp3_username = input("Enter HyP3 Username: ")
    hyp3_password = getpass.getpass("Enter HyP3 Password: ")
    hyp3 = HyP3(username=hyp3_username, password=hyp3_password)

    dest_bucket = args.bucket_path.split("/")[0]
    prefix_str = args.bucket_path.split("/", 1)[1]
    copy_processed_granules_to_bucket = True if args.bucket_path else False

    try:
        s3 = boto3.resource('s3')
    except:
        print("Error connecting to S3. Make sure your EC2 instance is able to access S3.")

    granules_group_dict = generate_granules_group_dict(args.csv)

    submit_granules(hyp3, granules_group_dict)
    
    for year_path_frame in granules_group_dict.keys():
        year, path_frame = year_path_frame.split('_', 1)
        granule_sources = get_granule_sources(hyp3, year, path_frame)

        if copy_processed_granules_to_bucket:
            try:
                print(year, path_frame, "copying jobs to bucket")
                copy_granules_to_bucket(s3, dest_bucket, prefix_str, year, path_frame, granule_sources)
                print(year, path_frame, "DONE copying jobs to bucket")
            except Exception as e:
                print("There was an error copying the granule from ASF to your bucket. Continuing to the next granule...")
                traceback.print_exc()

        else:
            print("Since you are not copying to your bucket, your processed ASF granules are available here:")
            for copy_source in granule_sources:
                print(f"{copy_source['bucket']}/{copy_source['key']}")
                print(f"Expiration Time: {copy_source['expiration_time']}")

    print("Done with everything.")

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
    user_input = input((
        "Enter 'Y' to confirm you would like to submit these granules, "
        "or 'N' if you have already submitted the granules and want to copy the processed granules to your bucket: "))
    if user_input.lower() != "y":
        return

    # submit the jobs for all year_path_frame's
    for year_path_frame,granule_name_list in granules_group_dict.items():
        for granule_name in granule_name_list:
                hyp3.submit_rtc_job(granule_name, year_path_frame, resolution=30.0, radiometry='gamma0',
                                    scale='power', speckle_filter=False, dem_matching=True,
                                    include_dem=True, include_inc_map=True, include_scattering_area=True)

    print("Jobs successfully submitted.")

def get_granule_sources(hyp3, year, path_frame):
    # returns a list of dictionaries where files are located on ASF's bucket
    year_path_frame = "_".join([year, path_frame])

    batch = hyp3.find_jobs(name=year_path_frame)
    batch = hyp3.watch(batch)

    # list of tuples, first item is dictionary with Bucket and Key, second item datetime of expiration time 
    return [({'Bucket':job.files[0]['s3']['bucket'],'Key':job.files[0]['s3']['key']}, job.expiration_time)  
            for job in batch.jobs]

def copy_granules_to_bucket(s3, dest_bucket, prefix_str, year, path_frame, granule_sources):
    for copy_source, expiration_time in granule_sources:
        filename = ntpath.basename(copy_source['Key'])
        destination_key = os.path.join(prefix_str, f'{year}/{path_frame}/{filename}')
        today = datetime.now(timezone.utc)
        if not today > expiration_time: # TODO: Today's date in utc
            try:
                s3.meta.client.copy(copy_source, Bucket=dest_bucket, Key=destination_key)
            except Exception as e:
                print("\nError copying processed granule to your bucket. Traceback:")
                print(traceback.print_exc())
                print(f"Failed granule: {copy_source}")

        else:
            # job is expired and cannot be copied
            pass

if __name__ == '__main__':
   main()

